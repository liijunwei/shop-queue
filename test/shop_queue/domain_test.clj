(ns shop-queue.domain-test
  (:require [clojure.test :refer :all]
            [shop-queue.domain :as d]))

(deftest take-maps-party-size-to-smallest-table
  (let [s (d/default-state)
        {:keys [state ticket]} (d/take-ticket s "store-1" 2 "remote")]
    (is (= "A001" (:id ticket)))
    (is (= :two (:table-type ticket)))
    (is (= :waiting (:status ticket)))
    (is (= 0 (:parties-ahead (d/status-by-token state "store-1" (:token ticket))))))
  (let [s (d/default-state)
        {:keys [ticket]} (d/take-ticket s "store-1" 4 "scan")]
    (is (= :large (:table-type ticket)))))

(deftest parties-ahead-counts-waiting-tickets
  (let [s (d/default-state)
        r1 (d/take-ticket s "store-1" 2 "remote")
        r2 (d/take-ticket (:state r1) "store-1" 2 "remote")]
    (is (= 0 (:parties-ahead (d/status-by-token (:state r2) "store-1" (:token (:ticket r1))))))
    (is (= 1 (:parties-ahead (d/status-by-token (:state r2) "store-1" (:token (:ticket r2))))))))

(deftest call-next-and-transitions
  (let [s (d/default-state)
        {:keys [state]} (d/take-ticket s "store-1" 2 "remote")
        {:keys [state ticket]} (d/call-next state "store-1" :two)]
    (is (= "A001" (:id ticket)))
    (is (= :called (:status ticket)))
    (let [seated (d/seat state "store-1" "A001")]
      (is (= :seated (:status (:ticket seated)))))))

(deftest closed-queue-rejects-new-ticket
  (let [s (d/default-state)
        {:keys [state]} (d/set-queue-status s "store-1" :two :closed)]
    (is (thrown? clojure.lang.ExceptionInfo
                 (d/take-ticket state "store-1" 2 "remote")))))
