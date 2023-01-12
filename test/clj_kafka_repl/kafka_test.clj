(ns clj-kafka-repl.kafka-test
  (:require [clj-kafka-repl.confirm :as confirm]
            [clj-kafka-repl.explicit-partitioner :as ep]
            [clj-kafka-repl.kafka :as sut]
            [clj-kafka-repl.kafka-utils :as kafka-utils :refer [ensure-topic]]
            [clj-kafka-repl.test-utils :refer [init-logging! random-id with-edn-consumer with-edn-producer with-kafka]]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log]))

(use-fixtures
  :once
  (fn [f]
    (init-logging!)
    (log/infof "==> Starting kafka container...")
    (with-kafka f)))

(deftest can-set-group-offsets-to-start
  (let [topic (random-id)]
    ; GIVEN a topic
    (ensure-topic topic 2)

    ; AND some messages pushed to the topic across all partitions
    (with-edn-producer
      (fn [producer]
        (kafka-utils/produce producer topic [(ep/m->to-explicit-partitionable {:value "message1-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message1-1"} 1)
                                             (ep/m->to-explicit-partitionable {:value "message2-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message2-1"} 1)])))

    (let [group-id (random-id)]
      ; AND a consumer starting from the end of the topic
      (with-edn-consumer
        topic group-id :end
        (fn [consumer]
          (.commitSync consumer)
          (is (= [[0 2] [1 2]] (sut/get-group-offsets topic group-id)))

          ; WHEN the consumer group offset is set to the start of all partitions
          (binding [confirm/*no-confirm?* true]
            (sut/set-group-offsets! topic group-id [[0 :start] [1 :start]] :consumer consumer))))

      ; THEN all the messages are consumed
      (with-edn-consumer
        topic group-id nil
        (fn [consumer]
          (is (= 4 (count (kafka-utils/poll* consumer :expected-msgs 4)))))))))

(deftest can-set-group-offsets-to-end
  (let [topic (random-id)]
    ; GIVEN a topic
    (ensure-topic topic 2)

    ; AND some messages pushed to the topic across all partitions
    (with-edn-producer
      (fn [producer]
        (kafka-utils/produce producer topic [(ep/m->to-explicit-partitionable {:value "message1-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message1-1"} 1)])))

    (let [group-id (random-id)]
      ; AND a consumer starting at the start of the topic
      (with-edn-consumer
        topic group-id :start
        (fn [consumer]
          (.commitSync consumer)
          (is (= [[0 0] [1 0]] (sut/get-group-offsets topic group-id)))

          ; WHEN the consumer group offset is set to the end of all partitions
          (binding [confirm/*no-confirm?* true]
            (sut/set-group-offsets! topic group-id [[0 :end] [1 :end]] :consumer consumer))
          (.commitSync consumer)))

      ; THEN the offsets are successfully reset to the end of the topic
      (is (= [[0 1] [1 1]] (sut/get-group-offsets topic group-id))))))

(deftest can-set-group-offset-to-absolute-offset
  (let [topic (random-id)]
    ; GIVEN a topic
    (ensure-topic topic 2)

    ; AND some messages pushed to the topic across all partitions
    (with-edn-producer
      (fn [producer]
        (kafka-utils/produce producer topic [(ep/m->to-explicit-partitionable {:value "message1-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message1-1"} 1)
                                             (ep/m->to-explicit-partitionable {:value "message2-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message2-1"} 1)])))

    (let [group-id (random-id)]
      ; AND a consumer starting at the start of the topic
      (with-edn-consumer
        topic group-id :start
        (fn [consumer]
          (.commitSync consumer)
          (is (= [[0 0] [1 0]] (sut/get-group-offsets topic group-id)))

          ; WHEN the consumer group offset is set to position 1 in 1 partition
          (binding [confirm/*no-confirm?* true]
            (sut/set-group-offsets! topic group-id [[0 1]] :consumer consumer))
          (.commitSync consumer)))

      ; THEN the offset for the specified partition is successfully reset
      (is (= [[0 1] [1 0]] (sut/get-group-offsets topic group-id)))

      (with-edn-consumer
        topic group-id nil
        (fn [consumer]
          (let [events (kafka-utils/poll* consumer :expected-msgs 3)]
            (is (= 3 (count events)))
            (is (= #{"message1-1" "message2-0" "message2-1"} (set (map :value events))))))))))

(deftest can-set-group-offsets-to-relative-offsets
  (let [topic (random-id)]
    ; GIVEN a topic
    (ensure-topic topic 2)

    ; AND some messages pushed to the topic across all partitions
    (with-edn-producer
      (fn [producer]
        (kafka-utils/produce producer topic [(ep/m->to-explicit-partitionable {:value "message1-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message1-1"} 1)
                                             (ep/m->to-explicit-partitionable {:value "message2-0"} 0)
                                             (ep/m->to-explicit-partitionable {:value "message2-1"} 1)])))

    (let [group-id (random-id)]
      ; AND a consumer starting at the start of the topic
      (with-edn-consumer
        topic group-id :end
        (fn [consumer]
          (.commitSync consumer)
          (is (= [[0 2] [1 2]] (sut/get-group-offsets topic group-id)))

          ; WHEN the consumer group offset is moved back 1 from current position
          (binding [confirm/*no-confirm?* true]
            (sut/set-group-offsets! topic group-id [[0 -1]] :consumer consumer))
          (.commitSync consumer)))

      ; THEN the offsets are successfully reset to the end of the topic
      (is (= [[0 1] [1 2]] (sut/get-group-offsets topic group-id)))

      (with-edn-consumer
        topic group-id nil
        (fn [consumer]
          (let [events (kafka-utils/poll* consumer)]
            (is (= 1 (count events)))
            (is (= #{"message2-0"} (set (map :value events))))))))))