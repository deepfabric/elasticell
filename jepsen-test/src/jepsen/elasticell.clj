(ns jepsen.elasticell
  (:gen-class)
  (:import [jepsen.generator Generator])
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
        (info node "is pd? " (contains? (:pd test) node) )
        ;(c/su (c/exec :start-cell.sh :debug))
        (info "start cell over!")
        (Thread/sleep 5000)
    )

    (teardown! [_ test node]
        (info node "tearing down cell")
        ;(c/su (c/exec :stop.sh :cell))
         )))

(defn parse-long
  "Parses a string to a Long. Passes through `nil`."
  [s]
  (when s (Long/parseLong s)))

(def server1-conn {:pool {:max-total 8} :spec {:host "n9" :port 6379 :timeout-ms 3000}})	;time out shreshold for the client connection or read ops
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

(defn client
  "A client for a single compare-and-set register"
  [conn]
  (reify client/Client
    (setup! [_ test node]
      (client (wcar* (car/flushall))
      ;(client (wcar* (car/set "start" ""))
))

    (invoke! [this test op]
      (let [[k v] (:value op)
            crash (if (= :read (:f op)) :fail :info)]
        (try 
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
              :final (assoc op :type :ok, :value (independent/tuple k nil))
              :hset (let [ [member value] v
                          _  (wcar*  (car/hset (str "hkey" k) member value))]
                          (assoc op :type, :ok))
              :hget (let [ [member _] v
                          res  (wcar* (car/hget (str "hkey" k) member))]
                          (assoc op :type, :ok :value (independent/tuple k [member (parse-long res)]))))
          (catch clojure.lang.ExceptionInfo e (do (assoc op :type :crash :error e)))
          (catch java.net.SocketTimeoutException e (do (assoc op :type :crash :error e)))
          (catch Exception e (do (info "Exception type: " (type e)) (assoc op :type :crash :error e)) )
        )
      )
    )

    (teardown! [_ test]
      ; If our connection were stateful, we'd close it here.
      ; Verschlimmbesserung doesn't hold a connection open, so we don't need to
      ; close it.
      )))

(defn final   [_ _] {:type :invoke, :f :final, :value nil})
(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})			;generate random value for write operation
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 10) (rand-int 10)]})
(defn hget [_ _] {:type :invoke, :f :hget, :value [(rand-int 10) nil]})
(defn hset [_ _] {:type :invoke, :f :hset, :value [(rand-int 10) (rand-int 10000)]})

(def tables (transient {}))

(defn tables-get  [k]   (get tables k))
(defn tables-set  [k v] (assoc! tables k v))
(defn tables-count [] (count tables))
(defn tables-print []  (prn (persistent! tables)))

(defrecord CellRegister [value hashtable]
  Model
  (step [r op]
    do  
        (condp = (:f op)
          :write (CellRegister. (:value op) hashtable)
          :cas   (let [[cur new] (:value op)]
                  (if (= cur value)
                    (CellRegister. new hashtable)
                    (inconsistent (str "can't CAS " value " from " cur
                                        " to " new))))
          :read  (if (or (nil? (:value op))
                        (= value (:value op)))
                  r
                  (inconsistent (str "can't read " (:value op)
                                      " from register " value)))
          :hset  (let [ [new-member new-value] (:value op)]
                    (do (tables-set new-member new-value)
                        (CellRegister. value  (tables-count))))
          :hget  (let [ [new-member new-value] (:value op)]
                    (if  (= (tables-get new-member) new-value)
                          r
                          (do ;(throw (ex-info (str "hget wrong, new-member " new-member
                              ;         " tables-get from register: " (tables-get new-member)
                              ;        " client get new-value " new-value) {}))
                              (inconsistent (str "hget wrong, new-member " new-member
                                        " tables-get from register: " (tables-get new-member)
                                        " client get new-value " new-value)))
                    ))
          :final (do (def tables (transient {})) r)
                          ))
  Object
  (toString [this] (pr-str "value:" value " hashtable:" hashtable)))

(defn elasticell-register
  "A cell register"
  ([] (elasticell-register nil))
  ([value] (do (info "call elasticell-register") (CellRegister. value 0)) ))

(defn nemesis-gen
  "A generator which emits a start after a t1 second delay, and then a stop
  after a t2 second delay."
  []
  (gen/seq [
	(gen/sleep 1)
        {:type :info :f :start}
	(gen/sleep 2)
        {:type :info :f :paused}
	(gen/sleep 3)
        {:type :info :f :stop}
	(gen/sleep 4)
        {:type :info :f :resumed}
       ])
)

(defn elasticell-test
  "Given an options map from the command-line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (info :opts opts)
  (merge tests/noop-test
         {:name "elasticell"
          :os debian/os
          :db (db "latest")
          :pd   #{"n5" "n6" "n1" "localhost"}
          :client (client nil)
                :nemesis (
            nemesis/compose {
            ;(nemesis/partition-majorities-ring)				;Every node can see a majority, but no node sees the *same* majority
                    ;(nemesis/partition-random-halves)
            {:start :start :stop :stop} (nemesis/partition-halves) 		;first partion is smaller
            ;(nemesis/clock-scrambler 1)		;
            {:paused :start :resumed :stop} (nemesis/hammer-time :redis-server)}	;pause and resume the proccess named redis-server
            )
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
                                  (range)		     ;generated key range from 0 to 1
                                  (fn [k]
                                    (gen/concat
                                      (->> (gen/mix [hset hget])
                                          (gen/stagger 1/100) ;average time interval between 2 client operation
                                          (gen/limit 160)     ;total operations of a single independent key
                                      )
                                      ;(gen/sleep 1)	     ;sleep 1 seconds after a single independent key operation; comment it if you don't want to pause the operation
                                      (gen/once {:type :invoke, :f :final})
                                    )
                                  )
                                )
                                (gen/nemesis
                                  (gen/concat
                                      (gen/start-stop 1 6) ;start network partition 1 seconds after previous nemesis stop, resume at 3 seconds later
                                      ;(nemesis-gen) 
                                  )
                                ;(gen/seq (cycle 
                                ;	  [(gen/sleep 1)   ;generate a nemesis loops until time limit
                                                  ;           {:type :info, :f :start}
                                ;	   (gen/sleep 2)
                                ;	      {:type :info, :f :paused}
                                ;	   (gen/sleep 3)
                                ;	      {:type :info, :f :resumed}
                                                  ;        (gen/sleep 4)
                                                  ;           {:type :info, :f :stop}
                                ;	  ]))
                                )
                                (gen/time-limit (:time-limit opts)))}
          opts))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn elasticell-test})
                   (cli/serve-cmd))
            args))
