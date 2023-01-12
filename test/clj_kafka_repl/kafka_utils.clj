(ns clj-kafka-repl.kafka-utils
  (:require [clj-kafka-repl.kafka :refer [normalize-config]]
            [clj-nippy-serde.serialization :as nser]
            [clojure.tools.logging :as log]
            [kafka-avro-confluent.deserializers :refer [->avro-deserializer]]
            [kafka-avro-confluent.serializers :refer [->avro-serializer]])
  (:import (io.confluent.kafka.schemaregistry.utils ZkUtils)
           (java.time Duration)
           (java.util Properties UUID)
           (java.util UUID)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)
           (org.I0Itec.zkclient ZkClient ZkConnection)))

(def ^:dynamic *convert-logical-types?* false)

(defn get-partition-count
  [kafka-config topic]
  (with-open [admin (-> kafka-config normalize-config (AdminClient/create))]
    (-> (.describeTopics admin [topic])
        (.values)
        (.entrySet)
        first
        (.getValue)
        (.get)
        (.partitions)
        (.size))))

(defn with-consumer
  [kafka-config value-deserializer topic group-id partition-count f & {:keys [seek-to]
                                                                       :or   {seek-to :end}}]
  (let [consumer-config  (-> kafka-config
                             (assoc :group.id group-id)
                             normalize-config)
        key-deserializer (StringDeserializer.)
        consumer         (KafkaConsumer. consumer-config
                                         key-deserializer
                                         value-deserializer)]
    (try
      (let [topic-partitions (->> (if (pos-int? partition-count)
                                    partition-count
                                    (get-partition-count kafka-config topic))
                                  (range 0)
                                  (map #(TopicPartition. topic %))
                                  vec)]
        (.assign consumer topic-partitions)

        (case seek-to
          :start (.seekToBeginning consumer [])
          :end (.seekToEnd consumer [])
          :noop)

        (doseq [tp topic-partitions]
          (.position consumer tp)))

      (f consumer)

      (finally
        (.close consumer)))))

(defn with-avro-consumer
  ([kafka-config schema-registry topic partition-count f]
   (let [group-id           (str (UUID/randomUUID))
         value-deserializer (->avro-deserializer schema-registry :convert-logical-types? *convert-logical-types?*)]
     (with-consumer kafka-config value-deserializer topic group-id partition-count f)))
  ([kafka-config schema-registry topic f]
   (with-avro-consumer kafka-config schema-registry topic :all f)))

(defn with-nippy-consumer
  ([kafka-config topic partition-count f]
   (let [group-id           (str (UUID/randomUUID))
         value-deserializer (nser/nippy-deserializer)]
     (with-consumer kafka-config value-deserializer topic group-id partition-count f)))
  ([kafka-config topic f]
   (with-nippy-consumer kafka-config topic :all f)))

(defn- ConsumerRecord->m [cr]
  (if (bytes? (.value cr))
    (.value cr)
    (-> (.value cr)
        (with-meta (merge {:offset    (.offset cr)
                           :partition (.partition cr)
                           :topic     (.topic cr)
                           :timestamp (.timestamp cr)
                           :key       (.key cr)}
                          (meta (.value cr)))))))

(defn poll*
  [consumer & {:keys [expected-msgs retries poll-timeout]
               :or   {expected-msgs 1
                      retries       1250
                      poll-timeout  10}}]
  (loop [received []
         retries  retries]
    (if (or (>= (count received) expected-msgs)
            (zero? retries))
      received
      (recur (concat received
                     (->> (.poll consumer (Duration/ofMillis poll-timeout))
                          (map ConsumerRecord->m)))
             (dec retries)))))

(defn ensure-topic
  [kafka-config topic partition-count]
  (with-open [admin (-> kafka-config normalize-config (AdminClient/create))]
    (.createTopics admin [(NewTopic. topic partition-count (short 1))])))

(defn with-producer
  [kafka-config value-serializer f]
  (let [producer-config (-> (merge kafka-config {:retries 3})
                            normalize-config)
        key-serializer  (StringSerializer.)
        producer        (KafkaProducer. producer-config key-serializer value-serializer)]
    (try
      (f producer)
      (finally
        (.close producer)))))

(defn with-avro-producer
  [kafka-config schema-registry schema f]
  (with-producer kafka-config (->avro-serializer schema-registry schema) f))

(defn with-nippy-producer
  [kafka-config f]
  (with-producer kafka-config (nser/nippy-serializer) f))

(defn produce
  ([producer topic records]
   (doseq [r records]
     (let [[k v] (if (vector? r)
                   r
                   [(str (UUID/randomUUID)) r])]
       (log/debugf "Producing record %s to key %s on topic %s." v k topic)
       (deref
         (.send producer
                (ProducerRecord. topic k v)))))

   (.flush producer))
  ([kafka-config value-serializer topic records]
   (with-producer kafka-config value-serializer
                  (fn [producer]
                    (produce producer topic records)))))

(defn produce-avro
  [kafka-config schema-registry schema topic & records]
  (apply produce kafka-config (->avro-serializer schema-registry schema) topic records))

(defn produce-nippy
  [kafka-config schema-registry schema topic & records]
  (apply produce kafka-config (->avro-serializer schema-registry schema) topic records))

