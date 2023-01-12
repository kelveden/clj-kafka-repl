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
        :args (s/cat :ch (s/or :consumer-channel ::consumer-channel
                               :channel ::channel)))

(defn poll!
  "Reads off the next value (if any) from the specified channel."
  [{:keys [channel]}]
  (async/poll! channel))

(s/fdef poll!
        :args (s/cat :consumer-channel ::consumer-channel)
        :ret any?)

(defn skip!
  "Consumes and skips n items from the consumer channel."
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
  [ch]
  (let [channel (if (s/valid? ::consumer-channel ch)
                  (:channel ch)
                  ch)]
    (async-protocols/closed? channel)))

(s/fdef closed?
        :args (s/cat :ch (s/or :consumer-channel ::consumer-channel
                               :channel ::channel)))

(defn- loop-channel-to-sink
  [channel sink-fn {:keys [pred n] :or {pred any?
                                        n    nil}}]
  (try
    (loop [next            (async/<!! channel)
           processed-count 0]
      (when next
        (let [process?  (pred next)
              new-count (cond-> processed-count process? inc)]
          (when process? (sink-fn next))

          ; Limit looping to n invocations of f
          (when (or (nil? n) (< new-count n))
            (recur (async/<!! channel) new-count)))))

    (catch Exception e
      (log/error e))))

(s/def ::sink-opts (s/* (s/alt :n (s/cat :opt #(= % :n) :value pos-int?)
                               :pred (s/cat :opt #(= % :pred) :value fn?))))

(defn writer-sink
  "Creates a new sink to a specified PrintWriter - e.g. *out* for stdout.

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:printer`           | `#(puget/pprint % {:print-color true})` | The function to use to print the content to the writer. |
  | `:pred`              | `any?`  | A predicate to filter messages from the input channel with. |
  | `:n`                 | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |"
  [^PrintWriter writer & {:keys [printer]
                          :or   {printer #(puget/pprint % {:print-color true})}
                          :as   opts}]
  (let [ch (async/chan)]
    (future
      (binding [*out* writer]
        (loop-channel-to-sink ch printer opts))
      (println :writer-sink-closed))
    ch))

(s/fdef writer-sink
        :args (s/cat :writer #(instance? PrintWriter %)
                     :opts ::sink-opts)
        :ret future?)

(defn file-sink
  "Creates a new sink to a specified file path. Writes all messages received to the sink using pprint (can be overridden
  with the printer option).

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:printer`           | `clojure.pprint/pprint` | The function to use to print the content to file. |
  | `:pred`              | `any?`  | A predicate to filter messages from the input channel with. |
  | `:n`                 | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |"
  [f & {:keys [printer]
        :or   {printer clojure.pprint/pprint}
        :as   opts}]
  (let [ch (async/chan)]
    (future
      (with-open [w (io/writer (io/file f))]
        (loop-channel-to-sink ch #(printer % w) opts)
        (println :file-sink-closed)))
    ch))

(s/fdef file-sink
        :args (s/cat :f (s/and string? (complement clojure.string/blank?))
                     :opts ::sink-opts)
        :ret future?)

(defn atom-sink
  "Creates a new sink to a specified atom initialised with [].

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:pred`              | `any?`  | A predicate to filter messages from the input channel with. |
  | `:n`                 | `nil`   | A limit to the number of messages that will be written to the sink before it closes. |"
  [^Atom a & opts]
  (let [ch (async/chan)]
    (future
      (loop-channel-to-sink ch #(swap! a conj %) opts)
      (println :atom-sink-closed))
    ch))

(s/fdef atom-sink
        :args (s/cat :atom #(instance? Atom %)
                     :opts ::sink-opts)
        :ret future?)

(defn to-sinks!
  "Starts sending data from the specified input channel to the specified sinks.

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:close-sinks?`      | `true`  | Whether to close all sinks once the input channel closes. |"
  [{:keys [channel]} sinks & {:keys [close-sinks?]
                              :or   {close-sinks? true}}]
  (future
    (try
      (loop [next (async/<!! channel)]
        (when next
          (doseq [sink sinks] (async/>!! sink next))
          (recur (async/<!! channel))))

      (println :sending-to-sinks-completed)

      (catch Exception e
        (log/error e))
      (finally
        (when close-sinks?
          (doseq [sink sinks] (async/close! sink)))))))

(defn to-file
  "Convenience function that handles creating a new file-sink and directing all content from the incoming consumer
  channel to the file. The sink will be closed as soon as the input channel is closed.

  See channel/file-sink for option details."
  [ch f & opts]
  (let [sink (apply file-sink (cons f (->> opts (into []) (flatten))))]
    (to-sinks! ch [sink])))

(s/fdef to-file
        :args (s/cat :ch ::consumer-channel
                     :f (s/and string? (complement clojure.string/blank?))
                     :opts ::sink-opts)
        :ret future?)

(defn to-stdout
  "Convenience function that handles creating a new writer-sink to *out* and directing all content from the incoming consumer
  channel to the writer. The sink will be closed as soon as the input channel is closed.

  See channel/writer-sink for option details."
  [ch & opts]
  (let [sink (apply writer-sink (cons *out* (->> opts (into []) (flatten))))]
    (to-sinks! ch [sink])))

(s/fdef to-stdout
        :args (s/cat :ch ::consumer-channel
                     :opts ::sink-opts)
        :ret future?)

(defn to-atom
  "Convenience function that handles creating a new atom-sink and appending all content from the incoming consumer
  channel to the atom. The sink will be closed as soon as the input channel is closed.

  See channel/atom-sink for option details."
  [ch a & opts]
  (let [sink (apply atom-sink (cons a (->> opts (into []) (flatten))))]
    (to-sinks! ch [sink])))

(s/fdef to-atom
        :args (s/cat :ch ::consumer-channel
                     :a #(instance? Atom %)
                     :opts ::sink-opts)
        :ret future?)

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :consumer-channel ::consumer-channel)
        :ret map?)
