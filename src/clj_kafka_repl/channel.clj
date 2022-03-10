(ns clj-kafka-repl.channel
  "core.async based functions for use with [[kafka/consume]]."
  (:require [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [puget.printer :as puget]
            [clojure.tools.logging :as log])
  (:import (clojure.lang Atom)
           (java.io PrintWriter)))

(s/def ::progress-fn fn?)
(s/def ::channel #(satisfies? async-protocols/Channel %))
(s/def ::tracked-channel (s/keys :req-un [::channel ::progress-fn]))

(defn close!
  "Closes the specified core.async channel."
  [{:keys [channel]}]
  (async/poll! channel)
  (async/close! channel))

(s/fdef close!
        :args (s/cat :tracked-channel ::tracked-channel))

(defn poll!
  "Reads off the next value (if any) from the specified channel."
  [{:keys [channel]}]
  (async/poll! channel))

(s/fdef poll!
        :args (s/cat :tracked-channel ::tracked-channel)
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
        :args (s/cat :tracked-channel ::tracked-channel
                     :n pos-int?)
        :ret any?)

(defn closed?
  "Returns flag indicating whether the specified channel is closed."
  [{:keys [channel]}]
  (async-protocols/closed? channel))

(s/fdef closed?
        :args (s/cat :tracked-channel ::tracked-channel))

(defn- loop-channel
  [channel fn]
  (try
    (loop [next (async/<!! channel)]
      (when next
        (fn next)
        (recur (async/<!! channel))))
    (catch Exception e
      (log/error e))))

(defmulti to
          "Provides facilities for piping the content of the tracked channel to a given target."
          (fn [_ target] (type target)))

(defmethod to PrintWriter [{:keys [channel]} writer]
  (future
    (binding [*out* writer]
      (loop-channel channel #(puget/pprint % {:print-color true})))))

(defmethod to String [{:keys [channel]} file-path]
  (future
    (with-open [w (io/writer (io/file file-path))]
      (loop-channel channel #(clojure.pprint/pprint % w)))))

(defmethod to Atom [{:keys [channel]} a]
  (future
    (loop-channel channel #(swap! a conj %))))

(defn to-stdout-filtered
  [{:keys [channel]} pred]
  (future
    (loop-channel channel #(when (pred %)
                             (puget/pprint % {:print-color true})))))

(defn to-stdout
  "Streams the contents of the specified to stdout."
  [tracked-channel]
  (to tracked-channel *out*))

(s/fdef to-stdout
        :args (s/cat :tracked-channel ::tracked-channel
                     :options (s/* (s/alt :print (s/cat :opt #(= % :print)
                                                        :value fn?)))))

(defn to-file
  "Streams the contents of the specified channel to the given file path."
  [tracked-channel f]
  (to tracked-channel f))

(s/fdef to-file
        :args (s/cat :tracked-channel ::tracked-channel
                     :f string?))

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :tracked-channel ::tracked-channel)
        :ret map?)
