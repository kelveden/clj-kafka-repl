(ns clj-kafka-repl.channel
  "core.async based functions for use with [[kafka/consumer-chan]]."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [puget.printer :as puget]
            [clojure.tools.logging :as log]
            [clojure.pprint])
  (:import (clojure.lang Atom)
           (java.io PrintWriter)))

(s/def ::progress-fn fn?)
(s/def ::channel #(satisfies? async-protocols/Channel %))
(s/def ::consumer-channel (s/keys :req-un [::channel ::progress-fn]))

(defn close!
  "Closes the specified channel/consumer-channel."
  [ch]
  (let [channel (if (s/valid? ::consumer-channel ch)
                  (:channel ch)
                  ch)]
    (async/close! channel)))

(s/fdef close!
        :args (s/cat :consumer-channel ::consumer-channel))

(defn poll!
  "Reads off the next value (if any) from the specified channel."
  [{:keys [channel]}]
  (async/poll! channel))

(s/fdef poll!
        :args (s/cat :consumer-channel ::consumer-channel)
        :ret any?)

(defn skip!
  "Consumes and skips n items from the channel."
  [{:keys [channel]} n]
  (loop [x 0 offset nil partition nil]
    (let [result {:count     x
                  :offset    offset
                  :partition partition}]
      (if (< x n)
        (if-let [message (deref (future (async/<!! channel)) 3000 nil)]
          (recur (inc x)
                 (:offset message)
                 (:partition message))
          result)
        result))))

(s/fdef skip!
        :args (s/cat :consumer-channel ::consumer-channel
                     :n pos-int?)
        :ret any?)

(defn closed?
  "Returns flag indicating whether the specified channel is closed."
  [{:keys [channel]}]
  (async-protocols/closed? channel))

(s/fdef closed?
        :args (s/cat :consumer-channel ::consumer-channel))

(defn- loop-channel
  [channel fn {:keys [pred transformer] :or {pred        any?
                                             transformer identity}}]
  (try
    (loop [next (async/<!! channel)]
      (when next
        (when (pred next)
          (fn (transformer next)))
        (recur (async/<!! channel))))
    (catch Exception e
      (log/error e))))

(defmulti to
          "Provides facilities for streaming the content of the tracked channel to a given target."
          (fn [_ target & opts] (type target)))

(defmethod to PrintWriter [{:keys [channel]} writer & {:keys [printer]
                                                       :or   {printer #(puget/pprint % {:print-color true})}
                                                       :as   opts}]
  (future
    (binding [*out* writer]
      (loop-channel channel printer opts))))

(defmethod to String [{:keys [channel]} file-path & {:keys [printer]
                                                     :or   {printer clojure.pprint/pprint}
                                                     :as   opts}]
  ; Stream to file
  (future
    (with-open [w (io/writer (io/file file-path))]
      (loop-channel channel #(printer % w) opts))))

(defmethod to Atom [{:keys [channel]} a & {:as opts}]
  (future
    (loop-channel channel #(swap! a conj %) opts)))

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :consumer-channel ::consumer-channel)
        :ret map?)
