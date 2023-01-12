(ns clj-kafka-repl.kafka-utils
  (:require
    [clj-kafka-repl.core :as core]
    [clj-kafka-repl.kafka :refer [normalize-config]]
    [clojure.tools.logging :as log])
  (:import (java.time Duration)
           (java.util UUID)
           (java.util UUID)
           (org.apache.kafka.clients.admin AdminClient NewTopic)
           (org.apache.kafka.clients.consumer KafkaConsumer)
           (org.apache.kafka.clients.producer KafkaProducer ProducerRecord)
           (org.apache.kafka.common TopicPartition)
           (org.apache.kafka.common.serialization StringDeserializer StringSerializer)))

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
  [topic partition-count]
  (with-open [admin (-> core/*config* :kafka-config normalize-config (AdminClient/create))]
    (log/infof "==> Ensuring topic %s." topic)
    (.createTopics admin [(NewTopic. topic partition-count (short 1))])))

(defn with-consumer
  [value-deserializer topic group-id partition-count f & {:keys [seek-to]
                                                          :or   {seek-to :end}}]
  (let [kafka-config     (:kafka-config core/*config*)
        consumer-config  (-> kafka-config
                             (assoc :group.id group-id)
                             normalize-config)
        _                (log/infof "==> Consumer config: %s" consumer-config)

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

(defn with-producer
  [value-serializer f]
  (let [kafka-config    (:kafka-config core/*config*)
        producer-config (-> (merge kafka-config {:retries 3 :partitioner.class "clj_kafka_repl.ExplicitPartitioner"})
                            normalize-config)
        _               (log/infof "==> Producer config: %s" producer-config)

        key-serializer  (StringSerializer.)
        producer        (KafkaProducer. producer-config key-serializer value-serializer)]
    (try
      (f producer)
      (finally
        (.close producer)))))

(defn produce
  [producer topic records]
  (doseq [r records]
    (let [[k v] (if (vector? r)
                  r
                  [(str (UUID/randomUUID)) r])]
      (log/debugf "Producing record %s to key %s on topic %s." v k topic)
      (deref
        (.send producer
               (ProducerRecord. topic k v)))))

  (.flush producer))
