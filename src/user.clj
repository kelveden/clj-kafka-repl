(ns user
  (:require [clj-kafka-repl.channel :as ch]
            [clj-kafka-repl.core :refer [load-config with]]
            [clj-kafka-repl.deserialization :as dser]
            [clj-kafka-repl.kafka :as kafka]
            [clj-kafka-repl.serialization :as ser]
            [clojure.repl :refer [dir-fn]]
            [clojure.spec.test.alpha :as stest]
            [clojure.term.colors :refer [cyan green magenta yellow]]
            [kaocha.repl :as kaocha]
            [cheshire.core :as json]
            [java-time :as jt])
  (:import (java.util UUID)))

(defn get-namespace-functions
  [ns]
  (->> (dir-fn ns)
       (map name)
       (sort)
       (map #(-> (symbol (name ns) %)
                 resolve
                 meta))))

(defn help
  ([]
   (->> ['kafka 'ch]
        (map help)
        vec)
   nil)
  ([ns]
   (println "----------------------")
   (println (magenta (name ns)))
   (println "----------------------")
   (println)

   (doseq [f (get-namespace-functions ns)]
     (when-not (:no-doc f)
       (doseq [arglist (:arglists f)]
         (println
           (str "(" (cyan (str (name ns) "/" (:name f))) " " (yellow arglist) ")")))
       (println (green (:doc f)))
       (println)))

   (println)))

(defn run-tests
  [& [ns]]
  (if ns
    (kaocha/run ns)
    (kaocha/run-all {:reporter [kaocha.report/dots]})))

(def my-schema
  {:type   "record"
   :name   "Whatever"
   :fields [{:name "thing"
             :type "string"}]})

(stest/instrument)
(load-config)


;
; Consuming examples. Before running, you will need:
;
; * A zookeeper + kafka + schema registry running locally - there's a docker-compose.yaml in the root for this purpose.
; * The following configuration in your ~/.clj-kafka-repl/config.edn:
;
; {:default-value-serializer   :string
;  :default-value-deserializer :string
;
;  :profiles
;  {:dev
;   {:kafka-config
;    {:bootstrap.servers "localhost:9092"},
;
;    :schema-registry-config
;    {:base-url "http://localhost:8081"}}}}
;
;


(comment
  ; List all the topics available
  (with :dev (kafka/get-topics))

  ; Start a consumer and producer dealing with the same topic
  (def c (with :dev (kafka/consumer-chan "testtopic")))
  (def p (with :dev (kafka/producer-chan "testtopic")))


  ; --- Produce and consume a simple piece of data

  ; Produce some data to testtopic
  (kafka/produce! p "key1" "some string")

  ; Verify that the data has arrived at our channel
  (ch/poll! c)

  ; Send some more data to the topic
  (doseq [x (range 2 20)
          :let [k (str x)
                v (str (UUID/randomUUID))]]
    (kafka/produce! p k v))

  ; --- Stream from topic to atom

  ; Create an atom to house our data
  (def a (atom []))

  ; Send the content of the topic to the atom
  (def f (ch/to c a))
  @a

  ; Close the consumer (which also realizes the "to file" future
  (ch/close! c)

  ; Closing the channel has automatically closed the loop that was streaming the content to the atom
  (realized? f)


  ; --- Stream messages to a file as JSON

  ; Restart the consumer at the beginning of the topic
  (def c (with :dev (kafka/consumer-chan "testtopic" :offset :start)))

  ; Start streaming the content of the topic to a file as raw JSON
  (def f (ch/to c "/workspace/temp/stuff"
                :transformer #(update % :type str)
                :printer json/generate-stream))

  ; If the transformer and printer weren't specified above then raw EDN would be pretty printed to the file instead

  (ch/close! c)


  ; --- Stream to stdout from offset 10 with predicate for filtering messages

  ; Restart the consumer again
  (def c (with :dev (kafka/consumer-chan "testtopic" :offset 10)))

  ; Pretty-print to stdout any messages matching the given predicate (i.e. the key is divisable by 3)
  (def f (ch/to c *out* :pred (fn [{:keys [offset]}]
                                (= 0 (mod offset 3)))))

  (ch/close! c)


  ; --- Avro demonstration

  ; Create an avro producer and consumer
  (def schema {:type   "record"
               :name   "Whatever"
               :fields [{:name "id"
                         :type "string"}
                        {:name "someDate"
                         :type {:type        "int"
                                :logicalType "date"}}
                        {:name "someDateTime"
                         :type {:type        "long"
                                :logicalType "timestamp-millis"}}]})
  (def p (with :dev (kafka/producer-chan "avrotopic" :value-serializer (kafka/->avro-serializer schema))))
  (def c (with :dev (kafka/consumer-chan "avrotopic" :offset :start :value-deserializer (kafka/->avro-deserializer))))

  (kafka/produce! p "3" {:id (str (UUID/randomUUID)) :someDate 19121 :someDateTime 1652116242321})

  ; Verify that the message has been consumed AND logical types (i.e. the date and timestamp) are converted from their
  ; raw representation to JVM-friendly representations.
  (ch/poll! c)

  (ch/close! c))
