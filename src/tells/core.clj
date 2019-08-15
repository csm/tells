(ns tells.core
  (:require [aleph.tcp :as tcp]
            [clojure.tools.cli :as cli]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:gen-class)
  (:import [java.net InetSocketAddress]
           [io.netty.channel ChannelPipeline]
           [io.netty.handler.codec LengthFieldBasedFrameDecoder]
           [io.netty.buffer ByteBuf ByteBufAllocator ByteBufUtil]
           [io.netty.handler.timeout ReadTimeoutHandler]
           [java.util.concurrent TimeUnit]
           [java.util.concurrent.atomic AtomicLong]))

(def bytes-read (AtomicLong. 0))
(def bytes-written (AtomicLong. 0))
(def total-connections (AtomicLong. 0))
(def connections (AtomicLong. 0))

(def optspec [["-c" "--connect SPEC" "Proxy connections to SPEC (addr:port)."
               :parse-fn #(let [parts (.split % ":" 2)]
                            (if (= 2 (count parts))
                              (InetSocketAddress. ^String (first parts) (Integer/parseInt (second parts)))
                              (throw (IllegalArgumentException. "invalid --connect spec"))))]
              ["-l" "--listen SPEC" "Listen on SPEC (bind-addr:port or just port)."
               :parse-fn #(let [parts (.split % ":" 2)]
                            (case (count parts)
                              1 (InetSocketAddress. (Integer/parseInt (first parts)))
                              2 (InetSocketAddress. ^String (first parts) (Integer/parseInt (second parts)))))]
              ["-s" "--size N" "Set maximum record size."
               :default 2048
               :parse-fn #(Integer/parseInt %)
               :validate-fn [pos-int?]]
              ["-t" "--timeout N" "Set read timeout in milliseconds."
               :default 60000
               :parse-fn #(Integer/parseInt %)
               :validate-fn [pos-int?]]
              ["-h" "--help" "Show this help and exit."]
              ["-v" "--verbose" "Increase verbosity."
               :default 0
               :update-fn inc]])

(defn add-record-length-decoder
  [^ChannelPipeline pipeline]
  (.addBefore pipeline "handler" "tls-length" (LengthFieldBasedFrameDecoder. 16384 3 2)))

(def +pool+ (ByteBufAllocator/DEFAULT))

(defn offsets
  [chunk length]
  (loop [offset 0
         offsets []]
    (if (>= offset length)
      offsets
      (recur (+ offset chunk) (conj offsets offset)))))

(defmethod print-method ByteBuf
  [^ByteBuf buf writer]
  (.write writer "#bytebuf{")
  (when (pos? (.refCnt buf))
    (.write writer (format ":ridx %d :widx %d :cap %d :bytes \"" (.readerIndex buf) (.writerIndex buf) (.capacity buf)))
    (.write writer (ByteBufUtil/hexDump buf))
    (.write writer "\""))
  (.write writer "}"))

(defn get-uint24
  [buffer offset]
  (bit-or (bit-shift-left (.getUnsignedByte buffer offset) 16)
          (bit-shift-left (.getUnsignedByte buffer (inc offset)) 8)
          (.getUnsignedByte buffer (+ offset 2))))

(defn split-handshakes
  [^ByteBuf buffer]
  (loop [handshakes []
         buffer buffer]
    (if (>= (.readableBytes buffer) 4)
      (let [hs-len (get-uint24 buffer 1)]
        (recur (conj handshakes (.slice buffer 0 (+ hs-len 4)))
               (.slice buffer (+ hs-len 4) (- (.readableBytes buffer) hs-len 4))))
      (concat handshakes (when (pos? (.readableBytes buffer)) buffer)))))

(defn pipe!
  [size input output]
  (s/consume-async
    (fn [^ByteBuf message]
      (tap> [:trace {:task ::pipe!
                     :phase :begin
                     :message message}])
      (let [content-type (.getByte message 0)
            version (.getUnsignedShort message 1)
            length (.getUnsignedShort message 3)]
        (tap> [:debug {:task ::pipe! :phase :reading-content
                       :content-type content-type
                       :version version
                       :length length}])
        (if (and (= 22 content-type) (> length size))
          (let [buffer (.slice message 5 length)
                handshakes (split-handshakes buffer)
                buffers (doall (mapcat (fn [handshake]
                                         (let [offsets (offsets size (.readableBytes handshake))]
                                           (doall (map (fn [offset]
                                                         (let [chunk (min size (- (.readableBytes handshake) offset))
                                                               buffer (.buffer +pool+ chunk)]
                                                           (.writeByte buffer content-type)
                                                           (.writeShort buffer version)
                                                           (.writeShort buffer chunk)
                                                           (.writeBytes buffer handshake offset chunk)
                                                           buffer))
                                                       offsets))))
                                       handshakes))]
              (tap> [:trace {:task ::pipe! :phase :made-buffers :output buffers}])
              (.addAndGet bytes-read (+ length 5))
              (d/loop [buffers buffers sent? true]
                (if-let [buffer (first buffers)]
                  (let [buflen (.readableBytes buffer)]
                    (d/chain
                      (s/put! output buffer)
                      (fn [sent?]
                        (when sent?
                          (.addAndGet bytes-written buflen)
                          (d/recur (rest buffers) sent?)))))
                  (do
                    (.release message)
                    (tap> [:trace {:task ::pipe! :phase :end}])
                    sent?))))
          (do
            (tap> [:trace {:task ::pipe! :phase :end :output message}])
            (d/chain
              (s/put! output message)
              (fn [r]
                (when r
                  (.addAndGet bytes-read (+ length 5))
                  (.addAndGet bytes-written (+ length 5)))
                r))))))
    input))

(defn server
  [{:keys [connect size timeout]}]
  (fn [in-conn conn-info]
    (.incrementAndGet total-connections)
    (.incrementAndGet connections)
    (tap> [:info {:task ::server :phase :connected :connection conn-info}])
    (let [out-conn-ref (atom nil)]
      (s/on-closed in-conn
                   (fn []
                     (.decrementAndGet connections)
                     (tap> [:info {:task ::server :phase :inbound-connection-closed :connection in-conn}])
                     (when-let [c @out-conn-ref]
                       (s/close! c))))
      (d/chain
        (d/catch
          (tcp/client {:remote-address     connect
                       :raw-stream?        true
                       :pipeline-transform (fn [^ChannelPipeline pipeline]
                                             (.addBefore pipeline "handler" "read-timeout" (ReadTimeoutHandler. timeout TimeUnit/MILLISECONDS))
                                             (add-record-length-decoder pipeline))})
          (fn [error]
            (tap> [:info {:task ::server :phase :error-connecting :error error}])
            (s/close! in-conn)))
        (fn [out-conn]
          (reset! out-conn-ref out-conn)
          (s/on-closed out-conn
                       (fn []
                         (tap> [:info {:task ::server :phase :outbound-connection-closed :connection in-conn}])
                         (s/close! in-conn)))
          (pipe! size in-conn out-conn)
          (pipe! size out-conn in-conn))))))

(def levels {:info 0 :debug 1 :trace 2})

(defn tap-logger
  [verbose]
  (fn [msg]
    (when-let [[level msg] msg]
      (when (<= (get levels level 3) verbose)
        (println (pr-str msg))))))

(defn start-stats-loop!
  []
  (doto (Thread. ^Runnable
                 (fn []
                   (loop []
                     (tap> [:info {:task ::stats :bytes-read (.get bytes-read)
                                   :bytes-written (.get bytes-written)
                                   :total-connections (.get total-connections)
                                   :connections (.get connections)}])
                     (Thread/sleep 30000)
                     (recur))))
    (.setDaemon true)
    (.start)))

(defn -main
  [& args]
  (let [options (cli/parse-opts args optspec)]
    (when (-> options :options :help)
      (println "Usage: tells.core -c HOST:PORT -l [ADDR:]PORT [-s N]")
      (println)
      (println (-> options :summary))
      (System/exit 0))
    (when (not-empty (:errors options))
      (doseq [error (:errors options)]
        (println "Error:" error))
      (System/exit 1))
    (add-tap (tap-logger (-> options :options :verbose)))
    (tap> [:info {:task ::main :phase :starting-up :options (:options options)}])
    (let [server (tcp/start-server (server (:options options))
                                   {:socket-address     (-> options :options :listen)
                                    :pipeline-transform (fn [^ChannelPipeline pipeline]
                                                          (.addBefore pipeline "handler" "read-timeout" (ReadTimeoutHandler. (-> options :options :timeout) TimeUnit/MILLISECONDS))
                                                          (add-record-length-decoder pipeline))
                                    :raw-stream?        true})]
      (start-stats-loop!)
      (tap> [:info {:task ::main :phase :server-started :port (aleph.netty/port server)}]))))
