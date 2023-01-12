(defproject ovotech/clj-kafka-repl "0.1.0"
  :dependencies [[bigsy/clj-nippy-serde "0.1.3"]
                 [cheshire "5.11.0"]
                 [clojure.java-time "1.2.0"]
                 [clojure-term-colors "0.1.0"]
                 [mvxcvi/puget "1.3.4"]
                 [org.apache.kafka/kafka-clients "2.7.2"]
                 [org.clojure/clojure "1.11.1"]
                 [org.clojure/core.async "1.6.673"]
                 [org.clojure/tools.logging "1.2.4"]
                 [org.slf4j/jcl-over-slf4j "2.0.6"]
                 [org.slf4j/jul-to-slf4j "2.0.6"]
                 [org.slf4j/log4j-over-slf4j "2.0.6"]
                 [org.slf4j/slf4j-api "2.0.6"]
                 [ovotech/kafka-avro-confluent "2.7.0-1"]]

  :plugins [[mvxcvi/whidbey "2.0.0"]
            [lein-codox "0.10.8"]]

  :middleware [whidbey.plugin/repl-pprint]

  :aliases {"kaocha" ["run" "-m" "kaocha.runner"]
            "org"    ["nsorg" "--replace"]}

  :codox {:output-path "docs"
          :namespaces  [clj-kafka-repl.kafka clj-kafka-repl.channel]
          :metadata    {:doc/format :markdown}
          :project     {:name "clj-kafka-repl", :version nil, :package nil}}

  :aot [clj-kafka-repl.explicit-partitioner]

  :repl-options {:welcome (println
                            (str
                              (clojure.term.colors/yellow "Welcome to the Kafka Tooling REPL. Type ")
                              (clojure.term.colors/magenta "(help)")
                              (clojure.term.colors/yellow " or ")
                              (clojure.term.colors/magenta "(help ns)")
                              (clojure.term.colors/yellow " for more information.")))}

  :profiles {:dev  {:dependencies   [[ch.qos.logback/logback-classic "1.4.5"]
                                     [ch.qos.logback/logback-core "1.4.5"]
                                     [lambdaisland/kaocha "1.72.1136"]
                                     [vise890/zookareg "2.7.0-1"]]

                    :eftest         {:multithread? false}
                    :resource-paths ["test/resources"]
                    :plugins        [[lein-eftest "0.4.2"]]

                    :repl-options   {:init-ns user}}})
