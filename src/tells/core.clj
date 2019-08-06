(ns tells.core
  (:require [aleph.tcp :as tcp]
            [clojure.tools.cli :as cli]
            [manifold.deferred :as d]
            [manifold.stream :as s])
  (:gen-class)
  (:import [java.net InetSocketAddress]
           [io.netty.channel ChannelPipeline]
           [io.netty.handler.codec LengthFieldBasedFrameDecoder]
           [io.netty.buffer ByteBuf ByteBufAllocator ByteBufUtil]))

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
          (let [split-message (.buffer +pool+)
                offsets (offsets size length)]
            (tap> [:debug {:task ::pipe! :phase :made-offsets :offsets offsets}])
            (loop [offsets offsets]
              (when-let [offset (first offsets)]
                (let [chunk (min size (- length offset))]
                  (.writeByte split-message content-type)
                  (.writeShort split-message version)
                  (.writeShort split-message chunk)
                  (.writeBytes split-message message (+ offset 5) chunk)
                  (recur (rest offsets)))))
            (tap> [:trace {:task ::pipe! :phase :end :output split-message}])
            (s/put! output split-message))
          (do
            (tap> [:trace {:task ::pipe! :phase :end :output message}])
            (s/put! output message)))))
    input))

(defn server
  [{:keys [connect size]}]
  (fn [in-conn conn-info]
    (tap> [:info {:task ::server :phase :connected :connection conn-info}])
    (let [out-conn-ref (atom nil)]
      (s/on-closed in-conn
                   (fn []
                     (tap> [:info {:task ::server :phase :inbound-connection-closed :connection in-conn}])
                     (when-let [c @out-conn-ref]
                       (s/close! c))))
      (d/chain
        (d/catch
          (tcp/client {:remote-address connect
                       :raw-stream? true
                       :pipeline-transform add-record-length-decoder})
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
                                    :pipeline-transform add-record-length-decoder
                                    :raw-stream?        true})]
      (tap> [:info {:task ::main :phase :server-started :port (aleph.netty/port server)}]))))
