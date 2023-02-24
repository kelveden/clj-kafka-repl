(ns clj-kafka-repl.kafka
  "Functions for consuming, producing and reading metadata from kafka."
  (:require [clj-kafka-repl.channel :as ch]
            [clj-kafka-repl.confirm :refer [with-confirmation]]
            [clj-kafka-repl.confirm :refer [with-confirmation] :as confirm]
            [clj-kafka-repl.core :refer [*config* *options* *profile* with]]
            [clj-kafka-repl.deserialization :as dser :refer [new-deserializer]]
            [clj-kafka-repl.serialization :as ser :refer [new-serializer]]
            [clojure.core.async :as async]
            [clojure.core.async.impl.protocols :as async-protocols]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [java-time :as jt]
            [kafka-avro-confluent.deserializers]
            [kafka-avro-confluent.serializers])
  (:import (java.util UUID)
           (java.util.concurrent TimeUnit)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (java.time Duration)
           (java.io Serializable)))

(def ^:private max-poll-records 500)

(defn- zoned-date-time-string?
  [s]
  (-> (and
        (string? s)
        (try
          (jt/zoned-date-time s)
          (catch Exception _ nil)))
      boolean))

(defn- instant-string?
  [s]
  (-> (and
        (string? s)
        (try
          (jt/instant s)
          (catch Exception _ nil)))
      boolean))

(defn ^:no-doc normalize-config
  [config]
  (->> config
       (clojure.walk/stringify-keys)
       (map (fn [[k v]] [k (if (number? v) (int v) v)]))
       (into {})))

(s/def ::zoned-date-time-string zoned-date-time-string?)
(s/def ::instant-string instant-string?)
(s/def ::non-blank-string (s/and string? (complement clojure.string/blank?)))
(s/def ::topic (s/or :keyword keyword?
                     :string ::non-blank-string))
(s/def ::group (s/or :keyword keyword?
                     :string ::non-blank-string))
(s/def ::partition nat-int?)
(s/def ::offset nat-int?)
(s/def ::partition-offset (s/tuple ::partition ::offset))

(s/def ::offset-specification (s/or :absolute ::offset
                                    :relative neg-int?
                                    :keyword #{:start :end}
                                    :timestamp ::zoned-date-time-string))
(s/def ::partition-offset-specification
  (s/cat :partition nat-int?
         :offset ::offset-specification))

(s/def ::key any?)
(s/def ::value any?)
(s/def ::timestamp ::zoned-date-time-string)
(s/def ::timestamp-type ::non-blank-string)
(s/def ::kafka-message (s/keys :req-un [::key ::partition ::offset ::value ::timestamp ::timestamp-type]))

(s/def ::bootstrap.servers ::non-blank-string)
(s/def ::kafka-config (s/keys :req-un [::bootstrap.servers]))

(defn- default-serializer
  [opts-key]
  (if-let [type (get *options* opts-key)]
    (new-serializer type)
    (new-serializer :string)))

(defn- default-deserializer
  [opts-key]
  (if-let [type (get *options* opts-key)]
    (new-deserializer type)
    (new-deserializer :string)))

(def ^:private default-key-serializer (partial default-serializer :default-key-serializer))
(def ^:private default-value-serializer (partial default-serializer :default-value-serializer))
(def ^:private default-key-deserializer (partial default-deserializer :default-key-deserializer))
(def ^:private default-value-deserializer (partial default-deserializer :default-value-deserializer))

(defn- ->topic-name
  [topic]
  (if-let [result (if (keyword? topic)
                    (get (:topics *config*) topic)
                    topic)]
    result
    (throw (Exception. (format "Could not determine name for topic %s." topic)))))

(defn- ->group-id
  [group]
  (if-let [result (if (keyword? group)
                    (get (:consumer-groups *config*) group)
                    group)]
    result
    (throw (Exception. (format "Could not determine id for consumer group %s." group)))))

(defn get-group-offset
  "Gets the offset of the given consumer group on the given topic/partition."
  [topic group partition]
  (let [group-id     (->group-id group)
        topic-name   (->topic-name topic)
        kafka-config (:kafka-config *config*)
        cc           (-> kafka-config
                         (merge {:group.id group-id})
                         normalize-config)
        new-consumer (KafkaConsumer. cc (new-deserializer :noop) (new-deserializer :noop))
        tp           (TopicPartition. topic-name partition)]
    (try
      (some-> (.committed new-consumer tp)
              (.offset))
      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-group-offset
        :args (s/cat :topic ::topic
                     :group ::group
                     :partition nat-int?)
        :ret nat-int?)

(defn get-topic-partitions
  "Gets the vector of partitions available for the given topic."
  [topic]
  (let [topic-name   (->topic-name topic)
        kafka-config (:kafka-config *config*)
        cc           (normalize-config kafka-config)
        new-consumer (KafkaConsumer. cc (new-deserializer :noop) (new-deserializer :noop))]
    (try
      (->> (.partitionsFor new-consumer topic-name)
           (map #(.partition %))
           sort
           vec)

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-topic-partitions
        :args (s/cat :topic ::topic)
        :ret (s/coll-of nat-int?))

(defn get-latest-offsets
  "Gets a vector of vectors representing the mapping of partition->latest-offset for
  the partitions of the given topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:partitions`      | `nil`   | Limit the results to the specified collection of partitions. |"
  [topic & {:keys [partitions]
            :or   {partitions nil}}]
  (let [topic-name       (->topic-name topic)
        kafka-config     (:kafka-config *config*)
        cc               (normalize-config kafka-config)
        new-consumer     (KafkaConsumer. cc (new-deserializer :noop) (new-deserializer :noop))
        topic-partitions (map #(TopicPartition. topic-name %)
                              (or partitions (get-topic-partitions topic-name)))]
    (try
      (->> (.endOffsets new-consumer topic-partitions)
           (map (fn [[tp o]]
                  [(.partition tp) o]))
           (sort-by first)
           vec)

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-latest-offsets
        :args (s/cat :topic ::topic
                     :overrides (s/* (s/alt :partitions (s/cat :opt #(= % :partitions)
                                                               :value (s/coll-of nat-int?)))))
        :ret (s/coll-of ::partition-offset))

(defn get-earliest-offsets
  "Gets a vector of vectors representing the mapping of partition->earliest-offset for
  the partitions of the given topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:partitions`      | `nil`   | Limit the results to the specified collection of partitions. |"
  [topic & {:keys [partitions]
            :or   {partitions nil}}]
  (let [topic-name       (->topic-name topic)
        kafka-config     (:kafka-config *config*)
        cc               (normalize-config kafka-config)
        new-consumer     (KafkaConsumer. cc (new-deserializer :noop) (new-deserializer :noop))
        topic-partitions (map #(TopicPartition. topic-name %)
                              (or partitions (get-topic-partitions topic-name)))]
    (try
      (->> (.beginningOffsets new-consumer topic-partitions)
           (map (fn [[tp o]]
                  [(.partition tp) o]))
           (sort-by first)
           (vec))

      (finally
        (.close new-consumer 0 TimeUnit/SECONDS)))))

(s/fdef get-earliest-offsets
        :args (s/cat :topic ::topic
                     :overrides (s/* (s/alt :partitions (s/cat :opt #(= % :partitions)
                                                               :value (s/coll-of nat-int?)))))
        :ret (s/coll-of ::partition-offset))

(defn get-group-offsets
  "Gets the offsets on all partitions of the given topic for the specified consumer group."
  [topic group]
  (->> (->topic-name topic)
       (get-topic-partitions)
       (map (fn [p]
              [p (get-group-offset topic group p)]))
       (remove (fn [[_ o]] (nil? o)))
       (vec)))

(s/fdef get-group-offsets
        :args (s/cat :topic ::topic
                     :group ::group)
        :ret (s/coll-of ::partition-offset))

(defn- set-group-offset!
  "Set the offset for the specified consumer group on the specified partition."
  [topic consumer group-id partition new-offset]
  ; IMPORTANT: For this to work you need make sure that no consumers in the same group are
  ; already running against this partition.
  (let [topic-name      (->topic-name topic)
        tp              (TopicPartition. topic-name partition)
        adjusted-offset (if (neg-int? new-offset)
                          (->> (.endOffsets consumer [tp])
                               (map (fn [[_ o]] (+ o new-offset)))
                               first)
                          new-offset)]
    (cond
      (= :end adjusted-offset)
      (do
        (log/debugf "Seeking to end of %s-%s in group %s." topic-name partition group-id)
        (.seekToEnd consumer [tp])
        (.position consumer tp))

      (= :start adjusted-offset)
      (do
        (log/debugf "Seeking to start of %s-%s in group %s." topic-name partition group-id)
        (.seekToBeginning consumer [tp])
        (.position consumer tp))

      :else
      (do
        (log/debugf "Seeking to %s of %s-%s in group %s." adjusted-offset topic-name partition group-id)
        (.seek consumer tp adjusted-offset)))

    (.commitSync consumer)))

(defn set-group-offsets!
  "Sets the offsets for the specified group on the specified topic to the offsets given in the passed
  sequence of partition->offset pairs. The offset in each pair can be one of several types:

  * A natural integer - an absolute offset.
  * A negative integer - an offset relative to the current offset (i.e. deduct from the current offset)
  * :start - seek to start.
  * :end - seek to end.
  * date-time string - set offset to that which was current at the given time."
  [topic group partition-offsets & {:keys [consumer] :or {consumer nil}}]
  (let [group-id         (->group-id group)
        topic-name       (->topic-name topic)
        kafka-config     (:kafka-config *config*)
        cc               (-> kafka-config
                             (merge {:group.id group-id})
                             normalize-config)
        create-consumer? (nil? consumer)
        new-consumer     (or consumer (KafkaConsumer. cc (new-deserializer :string) (new-deserializer :string)))
        topic-partitions (->> partition-offsets
                              (map first)
                              (map #(TopicPartition. topic-name %)))]
    (when create-consumer?
      (.assign new-consumer topic-partitions))

    (with-confirmation
      (format "You are about to set the group offsets for group %s on topic %s for %s partitions.
    Make sure that no other consumers on the same group are running before continuing." group-id topic-name (count partition-offsets))
      (try
        (doseq [[p o] partition-offsets]
          (set-group-offset! topic-name new-consumer group-id p o))
        (.commitSync new-consumer)
        (finally
          (when create-consumer?
            (.close new-consumer 0 TimeUnit/SECONDS)))))))

(s/fdef set-group-offsets!
        :args (s/cat :topic ::topic
                     :group ::group
                     :partition-offsets (s/coll-of ::partition-offset-specification)
                     :overrides (s/* (s/alt :consumer (s/cat :opt #(= % :consumer)
                                                             :value #(instance? KafkaConsumer %))))))

(defn- offsets-diff
  [current-offsets latest-offsets]
  (-> (map (fn [[p current-offset] [_ latest-offset]]
             [p (-> (or current-offset 0) (- latest-offset))])
           current-offsets
           latest-offsets)
      doall vec))

(defn- lag-sum
  [by-partition-lags]
  (- (reduce (fn [acc [_ x]] (+ acc x)) 0 by-partition-lags)))

(defn- to-lag-map
  [current-offsets latest-offsets]
  (let [consumer-group-partitions (set (map first current-offsets))
        refined-latest-offsets    (remove (fn [[p _]]
                                            (not (consumer-group-partitions p)))
                                          latest-offsets)
        lags                      (offsets-diff current-offsets refined-latest-offsets)]
    {:total-lag    (lag-sum lags)
     :by-partition lags
     :offsets      {:current current-offsets
                    :latest  refined-latest-offsets}}))

(defn get-lag
  "Gets the topic lag for the given consumer group.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:verbose?`        | `false` | If `true`, will include by-partition breakdown. |"
  [topic group & {:keys [verbose?] :or {verbose? true}}]
  (let [group-id        (->group-id group)
        topic-name      (->topic-name topic)
        current-offsets (get-group-offsets topic-name group-id)
        latest-offsets  (get-latest-offsets topic-name)]
    (if verbose?
      (-> (to-lag-map current-offsets latest-offsets)
          (assoc :topic topic-name))
      (-> (offsets-diff current-offsets latest-offsets)
          (lag-sum)))))

(s/fdef get-lag
        :args (s/cat :topic ::topic
                     :group ::group
                     :overrides (s/* (s/alt :verbose? (s/cat :opt #(= % :verbose?) :value boolean?))))
        :ret (s/or :map map? :lag int?))

(defn- cr->kafka-message
  [cr]
  {:key            (.key cr)
   :partition      (.partition cr)
   :offset         (.offset cr)
   :timestamp      (-> (.timestamp cr) jt/instant)
   :timestamp-type (str (.timestampType cr))
   :value          (.value cr)
   :type           (type (.value cr))})

(defn- random-group-id
  []
  (let [group-id (format "clj-kafka-repl-%s" (UUID/randomUUID))]
    (println (format "Will consume using group id %s..." group-id))
    group-id))

(defn consumer-chan
  "Opens a consumer over the specified topic and returns a ::ch/consumer-channel which is a wrapper over a core.async
  channel.

  The channel will stay open indefinitely unless either: a) the channel is explicitly closed using ch/close! or b)
  the specified message limit is reached.

  | key                  | default | description |
  |:---------------------|:--------|:------------|
  | `:partition`         | `nil`   | Limit consumption to a specific partition. |
  | `:offset`            | `:end`  | Start consuming from the specified offset. Valid values: `:start`, `:end`, numeric offset |
  | `:partition-offsets` | `nil`   | Vector of partition+offset vector pairs that represent a by-partition representation of offsets to start consuming from. |
  | `:key-deserializer`  | `nil`   | Deserializer to use to deserialize the message key. Overrides the value in config. Defaults to a string deserializer. |
  | `:value-deserializer`| `nil`   | Deserializer to use to deserialize the message value. Overrides the value in config. Defaults to a string deserializer. |
  | `:limit`             | `nil`   | The maximum number of messages to pull back either into the stream or the results vector (depending on stream mode). |
  | `:pred`              | `(constantly true)` | Predicate to filter each incoming message. |"
  [topic & {:keys [partition offset partition-offsets key-deserializer value-deserializer limit pred]
            :or   {partition          nil
                   offset             :end
                   partition-offsets  nil
                   key-deserializer   (default-key-deserializer)
                   value-deserializer (default-value-deserializer)
                   limit              nil
                   pred               (constantly true)}}]
  (let [topic-name       (->topic-name topic)
        kafka-config     (:kafka-config *config*)
        group-id         (random-group-id)
        cc               (-> kafka-config
                             (assoc :group.id group-id
                                    :max.poll.records (cond
                                                        (nil? limit) max-poll-records
                                                        (> limit max-poll-records) max-poll-records
                                                        :else limit))
                             normalize-config)
        consumer         (KafkaConsumer. cc key-deserializer value-deserializer)
        partitions       (cond
                           (some? partition-offsets) (map first partition-offsets)
                           (some? partition) [partition]
                           :else (map #(.partition %)
                                      (.partitionsFor consumer topic-name)))
        topic-partitions (map #(TopicPartition. topic-name %) partitions)]

    (.assign consumer topic-partitions)

    ;============================================
    ; Set the offsets for each partition
    ;============================================

    (binding [confirm/*no-confirm?* true]
      (cond
        (some? partition-offsets)
        (set-group-offsets! topic-name group-id partition-offsets :consumer consumer)

        (= :end offset)
        (.seekToEnd consumer topic-partitions)

        (= :start offset)
        (.seekToBeginning consumer topic-partitions)

        (neg-int? offset)
        (let [latest            (into {} (get-latest-offsets topic-name :partitions partitions))
              partition-offsets (->> partitions
                                     (map #(vector % (+ (get latest %) offset)))
                                     vec)]
          (set-group-offsets! topic-name group-id partition-offsets :consumer consumer))

        :else
        (let [earliest-offset   (apply min (map second (get-earliest-offsets topic-name :partitions partitions)))
              partition-offsets (vec (map #(vector % offset) partitions))]
          (if (< earliest-offset offset)
            (set-group-offsets! topic-name group-id partition-offsets :consumer consumer)
            (do
              (log/info "Specified offset is before the earliest offset. Therefore, will seek from beginning.")
              (.seekToBeginning consumer topic-partitions))))))

    ;============================================
    ; Start consuming
    ;============================================

    (let [count-atom
          (atom 0)

          progress
          (atom {:total-received  0
                 :total-remaining nil
                 :offsets         nil})

          ch
          (async/chan limit)

          profile
          *profile*

          consumer-channel
          {:channel     ch
           :progress-fn #(with profile
                               (let [{:keys [by-partition total-received]}
                                     @progress

                                     current-offsets
                                     (->> (into [] by-partition)
                                          (sort-by first))

                                     latest-offsets
                                     (get-latest-offsets topic-name)]
                                 (-> (to-lag-map current-offsets latest-offsets)
                                     (assoc :total-received total-received))))}]
      (future
        (try
          (loop []
            (let [messages (->> (.poll consumer (Duration/ofMillis 2000))
                                (map cr->kafka-message))
                  filtered (->> messages
                                (filter pred)
                                (take (- (or limit Long/MAX_VALUE)
                                         @count-atom)))]

              (doseq [{:keys [partition offset]} messages]
                (swap! progress #(-> %
                                     (assoc-in [:by-partition partition] (inc offset))
                                     (update :total-received inc))))

              ; Push filtered messages to the channel
              (doseq [msg filtered]
                (when (not (async-protocols/closed? ch))
                  (swap! count-atom inc)
                  (async/>!! ch msg)))

              (when (and (not (async-protocols/closed? ch))
                         (or (nil? limit)
                             (< @count-atom limit)))
                (recur))))

          (catch Throwable e
            (println e)
            (log/error e))

          (finally
            (.close consumer 0 TimeUnit/SECONDS)
            (async/close! ch)
            (println "Consumer closed."))))

      consumer-channel)))

(s/fdef consumer-chan
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :limit (s/cat :opt #(= % :limit) :value pos-int?)
                                       :partition (s/cat :opt #(= % :partition) :value nat-int?)
                                       :partition-offsets (s/coll-of ::partition-offset-specification)
                                       :offset (s/cat :opt #(= % :offset) :value ::offset-specification)
                                       :key-deserializer (s/cat :opt #(= % :key-deserializer) :value ::dser/deserializer)
                                       :value-deserializer (s/cat :opt #(= % :value-deserializer) :value ::dser/deserializer)
                                       :predicate (s/cat :opt #(= % :predicate) :value (s/or :string string? :fn fn?)))))
        :ret ::ch/consumer-channel)

(defn get-offsets-at
  "Attempts to calculate the earliest offset after the specified point in time. 'at' should be a string representation of an
  instant; e.g. 2000-01-01T00:00:00Z.

  The returned map pairs each partition with the matching offset and timestamp of that offset. There is also an indication
  as what the earliest overall offset is by both offset and timestamp."
  [topic at]
  (let [topic-name   (->topic-name topic)
        at-millis    (jt/to-millis-from-epoch (jt/instant at))

        kafka-config (:kafka-config *config*)
        group-id     (random-group-id)
        cc           (-> kafka-config
                         (assoc :group.id group-id)
                         (normalize-config))]
    (with-open [c (KafkaConsumer. cc (default-key-deserializer) (default-value-deserializer))]
      (let [topic-partitions (->> (.partitionsFor c topic-name)
                                  (reduce (fn [acc p]
                                            (assoc acc (TopicPartition. topic-name (.partition p)) at-millis))
                                          {}))
            offsets          (->> (.offsetsForTimes c topic-partitions)
                                  (map (fn [[tp ot]]
                                         [(.partition tp) [(.offset ot) (str (jt/instant (.timestamp ot)))]]))
                                  (sort-by first))]
        {:offsets               offsets
         :earliest-by-offset    (->> (rest offsets)
                                     (reduce (fn [[_ [o _] :as curr]
                                                  [_ [o' _] :as new]]
                                               (if (< o' o) new curr))
                                             (first offsets)))
         :earliest-by-timestamp (->> (rest offsets)
                                     (reduce (fn [[_ [_ t] :as curr]
                                                  [_ [_ t'] :as new]]
                                               (if (jt/before? (jt/instant t') (jt/instant t)) new curr))
                                             (first offsets)))}))))

(s/fdef get-offsets-at
        :args (s/cat :topic ::topic
                     :at ::instant-string)
        :ret map?)

(defn sample
  "Convenience function around [[kafka/consumer-chan]] to just sample a message from the topic."
  [topic & opts]
  (let [topic-name (->topic-name topic)
        c          (apply consumer-chan (concat [topic-name :limit 1 :offset :start] opts))]
    (try
      (deref
        (future
          (loop [m (ch/poll! c)]
            (if m
              m
              (do (Thread/sleep 100)
                  (recur (ch/poll! c))))))
        10000 nil)
      (finally
        (ch/close! c)))))

(s/fdef sample
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :deserializer (s/cat :opt #(= % :deserializer) :value ::dser/deserializer))))
        :ret map?)

(defn get-message
  "Gets the message at the specified offset on the given topic (if any).

  | key                   | default | description |
  |:----------------------|:--------|:------------|
  | `:partition`          | `nil`   | Limit consumption to a specific partition. |
  | `:value-deserializer` | `nil`   | Deserializer to use to deserialize the message value. |"
  [topic offset & {:keys [value-deserializer partition]
                   :or   {value-deserializer nil
                          partition          nil}}]
  (let [topic-name (->topic-name topic)
        args       (concat [topic-name
                            :offset (dec offset)
                            :limit 1
                            :pred #(= offset (:offset %))]
                           (when (some? partition) [:partition partition])
                           (when (some? value-deserializer) [:value-deserializer value-deserializer]))
        ch         (apply consumer-chan args)
        f          (future
                     (loop [m (ch/poll! ch)]
                       (if m
                         m
                         (recur (ch/poll! ch)))))]
    (try
      (deref f 5000 nil)
      (finally
        (future-cancel f)
        (ch/close! ch)))))

(s/fdef get-message
        :args (s/cat :topic ::topic
                     :offset ::offset-specification
                     :args (s/* (s/alt :deserializer (s/cat :opt #(= % :deserializer) :value ::dser/deserializer)
                                       :partition (s/cat :opt #(= % :partition) :value ::partition))))
        :ret map?)

(defn producer-chan
  "Produce messages to the specified topic.

  | key                | default | description |
  |:-------------------|:--------|:------------|
  | `:key-serializer`  | `nil`   | Serializer to use to serialize the message key. Will use a string deserializer if not specified. |
  | `:value-serializer`| `nil`   | Serializer to use to serialize the message value. Will use an edn serializer if not specified. |"
  ([topic & {:keys [key-serializer value-serializer]
             :or   {key-serializer   (default-key-serializer)
                    value-serializer (default-value-serializer)}}]
   (let [topic-name      (->topic-name topic)
         kafka-config    (:kafka-config *config*)
         producer-config (normalize-config kafka-config)
         producer        (KafkaProducer. producer-config key-serializer value-serializer)
         ch              (async/chan)]

     (future
       (try
         (loop [next (async/<!! ch)]
           (when next
             (case next
               :flush (.flush producer)

               (let [[k v] next
                     record (ProducerRecord. topic-name k v)]
                 (log/debugf "Producing record %s to key %s on topic %s." v k topic-name)
                 (.send producer record)))
             (recur (async/<!! ch))))
         (catch Exception e
           (prn e))
         (finally
           (.close producer)
           (println "Producer closed."))))
     ch)))

(s/fdef producer-chan
        :args (s/cat :topic ::topic
                     :args (s/* (s/alt :key-serializer (s/cat :opt #(= % :key-serializer) :value ::ser/serializer)
                                       :value-serializer (s/cat :opt #(= % :value-serializer) :value ::ser/serializer))))
        :ret ::ch/channel)

(defn produce!
  "Produces the given key/value pair to the given channel created with [[kafka/producer-chan]]."
  [channel key value]
  (async/>!! channel [key value]))

(s/fdef produce!
        :args (s/cat :channel ::ch/channel
                     :key #(instance? Serializable %)
                     :value #(instance? Serializable %))
        :ret boolean?)

(defn get-topics
  "Lists the names of all topics.

  | key        | default | description |
  |:-----------|:--------|:------------|
  | `:search`  | `nil`   | String to filter topics on. Only topic names containing the string will be returned. |"
  [& {:keys [search]}]
  (let [kafka-config (-> (:kafka-config *config*)
                         (assoc :request.timeout.ms 10000))
        cc           (-> kafka-config
                         normalize-config)
        deserializer (dser/new-deserializer :string)]

    (with-open [c (KafkaConsumer. cc deserializer deserializer)]
      (->> (.listTopics c)
           (keys)
           (filter #(or (not search)
                        (clojure.string/includes? % search)))
           (sort)))))

(s/fdef get-topics
        :args (s/cat :args (s/* (s/alt :search (s/cat :opt #(= % :search) :value ::non-blank-string))))
        :ret (s/coll-of ::non-blank-string))

(defn ->avro-serializer
  "Creates a new serializer for use in producing avro messages."
  [schema]
  (kafka-avro-confluent.serializers/->avro-serializer (:schema-registry-config *config*) schema))

(s/fdef ->avro-serializer
        :args (s/cat :schema coll?)
        :ret ::ser/serializer)

(defn ->avro-deserializer
  "Creates a new deserializer for use in consuming avro messages."
  []
  (kafka-avro-confluent.deserializers/->avro-deserializer (:schema-registry-config *config*)))

(s/fdef ->avro-deserializer
        :ret ::dser/deserializer)