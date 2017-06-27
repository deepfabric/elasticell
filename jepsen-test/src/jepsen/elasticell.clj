(ns jepsen.elasticell
  (:gen-class)
  (:import [knossos.model Model])
  (:require [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [taoensso.carmine :as car]
            [knossos.model :refer [inconsistent]]
            [jepsen [checker :as checker]
                    [cli :as cli]
                    [client :as client]
                    [control :as c]
                    [db :as db]
                    [generator :as gen]
                    [independent :as independent]
                    [nemesis :as nemesis]
                    [tests :as tests]
                    [util :as util :refer [timeout]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]))

(defn db
  "elasticell DB."
  [version]
  (reify db/DB
    (setup! [_ test node]
        (info node "start cell" version)
        (c/su (c/exec :start-cell.sh :debug))
        (info "start cell over!")
        (Thread/sleep 5000)
    )

    (teardown! [_ test node]
        (info node "tearing down cell")
        (c/su (c/exec :stop.sh :cell))
         )))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(def server1-conn {:pool {:max-total 8} :spec {:host "10.214.160.200" :port 6379}}) 
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      ;(client (wcar* (car/flushall))
      (client (wcar* (car/set "start" ""))
    ))

    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)]
          (case (:f op)
           :read (  let[value (wcar* (car/get k))]
                    (assoc op :type :ok, :value (independent/tuple k (parse-long value))))

            :write (do  (wcar* (car/set k v))
                        (assoc op :type, :ok))

            :cas (let [[value value'] v
                        res (wcar* (car/compare-and-set k value value'))]
                   (assoc op :type (if (= res 1)
                                     :ok
                                     :fail)))

            :hset (let [ [member value] v
                        _  (wcar*  (car/hset (str "hkey" k) member value))]
                        (assoc op :type, :ok))
            :hget (let [ [member _] v
                        res  (wcar* (car/hget (str "hkey" k) member))]
                        (assoc op :type, :ok :value (independent/tuple k [member (parse-long res)])))
                                     )))

    (teardown! [_ test]
      ; If our connection were stateful, we'd close it here.
      ; Verschlimmbesserung doesn't hold a connection open, so we don't need to
      ; close it.
      )))


(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 100)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 100) (rand-int 100)]})
(defn hget [_ _] {:type :invoke, :f :hget, :value [(rand-int 100) nil]})
(defn hset [_ _] {:type :invoke, :f :hset, :value [(rand-int 100) (rand-int 100)]})

(defrecord CellRegister [value hashtable list set zset]
  Model
  (step [r op]
    do  
        (condp = (:f op)
          :write (CellRegister. (:value op) hashtable list set zset)
          :cas   (let [[cur new] (:value op)]
                  (if (= cur value)
                    (CellRegister. new hashtable list set zset)
                    (inconsistent (str "can't CAS " value " from " cur
                                        " to " new))))
          :read  (if (or (nil? (:value op))
                        (= value (:value op)))
                  r
                  (inconsistent (str "can't read " (:value op)
                                      " from register " value)))
          :hset  (let [[new-member new-value] (:value op)]
                     (CellRegister. value (assoc hashtable new-member new-value) list set zset ))
          :hget  (let [[new-member new-value] (:value op)]
                    (if (= (get hashtable new-member) new-value)
                          r
                          (inconsistent (str "can't read " (:value op)
                                        " from register " value))))
                          ))
  Object
  (toString [this] (pr-str "value:" value " hashtable:" hashtable)))

(defn elasticell-register
  "A register"
  ([] (elasticell-register nil))
  ([value] (CellRegister. value {} nil nil nil)))

(defn elasticell-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "elasticell"
          :os debian/os
          :db (db "latest")
          :client (client nil)
          :nemesis (nemesis/partition-random-halves)
          :model  (elasticell-register)
          :checker (checker/compose
                     {
                      :perf     (checker/perf)
                      :indep (independent/checker
                               (checker/compose
                                 {:timeline (timeline/html)
                                  :linear   (checker/linearizable)}))})
          :generator (->> (independent/concurrent-generator
                            1
                            (range)
                            (fn [k]
                              (->> (gen/mix [r w])
                                   (gen/stagger 1/100)
                                   (gen/limit 500))))
                          (gen/nemesis
                            (gen/seq (cycle [(gen/sleep 4)
                                             {:type :info, :f :start}
                                             (gen/sleep 4)
                                             {:type :info, :f :stop}])))
                          (gen/time-limit (:time-limit opts)))}
         opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn elasticell-test})
                   (cli/serve-cmd))
            args))
