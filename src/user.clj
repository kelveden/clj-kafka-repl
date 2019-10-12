(ns user
  (:require [clojure.repl :refer [dir-fn]]
            [clojure.spec.test.alpha :as stest]
            [clojure.term.colors :refer [cyan yellow green magenta]]
            [clj-kafka-repl.kafka :as kafka]
            [clj-kafka-repl.channel :as ch])
  (:import [sun.security.tools.keytool Main]))

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

(defn create-keystore
  []
  (Main/main (into-array String [])))

(stest/instrument)

