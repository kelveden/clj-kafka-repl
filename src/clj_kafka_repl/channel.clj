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
           (java.io PrintWriter)
           (java.util UUID)))

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
  [channel fn {:keys [pred transformer n] :or {pred        any?
                                               transformer identity
                                               n           nil}}]
  (try
    (loop [next            (async/<!! channel)
           processed-count 0]
      (when next
        (let [process?  (pred next)
              new-count (cond-> processed-count process? inc)]
          (when process?
            (fn (transformer next)))

          ; Limit looping to n invocations of f
          (when (or (nil? n) (< new-count n))
            (recur (async/<!! channel) new-count)))))

    (catch Exception e
      (log/error e))))

(defmacro as-future
  [& body]
  `(let [fid# (str (UUID/randomUUID))]
    {:id     fid#
     :future (future
               ~@body
               (println {:future-completed fid#}))}))

(defmulti to
          "Provides facilities for streaming the content of the tracked channel to a given target."
          (fn [_ target & opts] (type target)))

(defmethod to PrintWriter [{:keys [channel]} writer {:keys [printer]
                                                     :or   {printer #(puget/pprint % {:print-color true})}
                                                     :as   opts}]
  (as-future
    (binding [*out* writer]
      (loop-channel channel printer opts))))

(defmethod to String [{:keys [channel]} file-path {:keys [printer]
                                                   :or   {printer clojure.pprint/pprint}
                                                   :as   opts}]
  ; Stream to file
  (as-future
    (with-open [w (io/writer (io/file file-path))]
      (loop-channel channel #(printer % w) opts))))

(defmethod to Atom [{:keys [channel]} a opts]
  (as-future
    (loop-channel channel #(swap! a conj %) opts)))

(s/def ::to-opts (s/* (s/alt :n (s/cat :opt #(= % :n) :value pos-int?)
                             :pred (s/cat :opt #(= % :pred) :value fn?))))

(defn to-file
  "Writes all messages received in the channel to the specified file path using pprint. To use your own pretty printing function,
  use `(to consumer-channel f :printer <your print function>)`."
  [consumer-channel f & opts]
  (to consumer-channel f opts))

(s/fdef to-file
        :args (s/cat :consumer-channel ::consumer-channel
                     :f (s/and string? (complement clojure.string/blank?))
                     :opts ::to-opts)
        :ret future?)

(defn to-stdout
  "Writes all messages received in the channel to stdout using pprint."
  [consumer-channel & opts]
  (to consumer-channel *out* opts))

(s/fdef to-stdout
        :args (s/cat :consumer-channel ::consumer-channel
                     :opts ::to-opts)
        :ret future?)

(defn to-atom
  "Appends all messages received in the channel to the specified atom using conj. This means that the atom should be
  initialised with an empty collection - e.g. `(atom [])`"
  [consumer-channel a & opts]
  (to consumer-channel a opts))

(s/fdef to-atom
        :args (s/cat :consumer-channel ::consumer-channel
                     :a #(instance? Atom %)
                     :opts ::to-opts)
        :ret future?)

(defn progress
  "Gives an indication of the progress of the given tracked channel on its partitions."
  [{:keys [progress-fn]}]
  (progress-fn))

(s/fdef progress
        :args (s/cat :consumer-channel ::consumer-channel)
        :ret map?)
