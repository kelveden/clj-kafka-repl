(ns clj-kafka-repl.test-utils
  (:require
    [clj-kafka-repl.core :as core]
    [clj-kafka-repl.kafka-utils :refer [with-consumer with-producer]]
    [clj-kafka-repl.test-utils]
    [clj-nippy-serde.serialization :as nser]
    [clojure.edn :as edn]
    [clojure.tools.logging :as log]
    [kafka-avro-confluent.serializers :refer [->avro-serializer]]
    [kafka-avro-confluent.deserializers :refer [->avro-deserializer]])
  (:import (java.util UUID)
           (org.apache.kafka.common.serialization Deserializer Serializer)
           org.slf4j.bridge.SLF4JBridgeHandler
           (org.testcontainers.containers KafkaContainer)
           (org.testcontainers.utility DockerImageName)))

(def ^:dynamic *convert-logical-types?* false)

(defn random-id
  []
  (str (UUID/randomUUID)))

(def edn-serializer
  (reify Serializer
    (configure [_ _ _])
    (serialize [_ _ data] (.getBytes (str data) "UTF-8"))
    (close [_])))

(def edn-deserializer
  (reify Deserializer
    (configure [_ _ _])
    (deserialize [_ _ data] (edn/read-string (String. data "UTF-8")))
    (close [_])))

(defn with-edn-consumer
  [topic group-id seek-to f]
  (with-consumer edn-deserializer topic group-id nil f :seek-to seek-to))

(defn with-edn-producer
  [f]
  (with-producer edn-serializer f))

(defn with-avro-consumer
  ([schema-registry topic partition-count f]
   (let [group-id           (str (UUID/randomUUID))
         value-deserializer (->avro-deserializer schema-registry :convert-logical-types? *convert-logical-types?*)]
     (with-consumer value-deserializer topic group-id partition-count f)))
  ([schema-registry topic f]
   (with-avro-consumer schema-registry topic :all f)))

(defn with-avro-producer
  [schema-registry schema f]
  (with-producer (->avro-serializer schema-registry schema) f))

(defn with-nippy-consumer
  ([topic partition-count f]
   (let [group-id           (str (UUID/randomUUID))
         value-deserializer (nser/nippy-deserializer)]
     (with-consumer value-deserializer topic group-id partition-count f)))
  ([topic f]
   (with-nippy-consumer topic :all f)))

(defn with-nippy-producer
  [f]
  (with-producer (nser/nippy-serializer) f))

(defn init-logging! []
  (SLF4JBridgeHandler/removeHandlersForRootLogger)
  (SLF4JBridgeHandler/install))

(defn with-kafka
  [f]
  (let [kafka             (doto (KafkaContainer. (DockerImageName/parse "confluentinc/cp-kafka:6.2.1"))
                            (.start))
        bootstrap-servers (-> (.getBootstrapServers kafka)
                              (clojure.string/replace "PLAINTEXT://" ""))
        core-config       {:bootstrap.servers bootstrap-servers}]
    (try
      (binding [core/*config*  {:kafka-config core-config}
                core/*options* {}]
        (log/infof "==> Using kafka config %s." (:kafka-config core/*config*))
        (f))
      (finally
        (.stop kafka)
        (log/infof "==> Stopped kafka container.")))))