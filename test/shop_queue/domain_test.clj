(ns shop-queue.domain-test
  (:require [clojure.test :refer [deftest is testing]]
            [shop-queue.domain :as d]))

(def now 1700000000)

(deftest table-type-matching
  (is (= :two (d/match-table-type d/default-table-types 1)))
  (is (= :two (d/match-table-type d/default-table-types 2)))
  (is (= :three (d/match-table-type d/default-table-types 3)))
  (is (= :large (d/match-table-type d/default-table-types 4)))
  (is (= :large (d/match-table-type d/default-table-types 20)))
  (is (nil? (d/match-table-type (dissoc d/default-table-types :large) 10))))

(deftest create-ticket
  (let [r (d/create-ticket {} 2 "remote" now "tok1" "id-1")]
    (is (:ok r))
    (is (= "id-1" (:id (:value r))))
    (is (= :two (:table-type (:value r))))
    (is (= 0 (:parties-ahead (:value r))))
    (is (= :waiting (get-in (:state r) [:tickets "id-1" :status])))
    (is (= 1 (get-in (:state r) [:queues :two :counter])))))

(deftest create-ticket-validation
  (is (not (:ok (d/create-ticket {} 0 "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 51 "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 2 "bogus" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 100 "remote" now "t" "id")))))

(deftest lifecycle
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "scan" now "tok2" "id-2")
        s2 (:state r2)]
    (is (= "id-2" (:id (:value r2))))
    (is (= 1 (:parties-ahead (:value r2))))

    (let [r3 (d/call-next s2 :two now)
          s3 (:state r3)]
      (is (= "id-1" (:id (:value r3))))
      (is (= :called (get-in s3 [:tickets "id-1" :status])))
      (is (= "id-1" (:id (d/current-called s3 :two))))
      (is (not (:ok (d/call-next s3 :two now))))

      (let [r4 (d/seat s3 :two "id-1" now)
            s4 (:state r4)]
        (is (:ok r4))
        (is (= :seated (get-in s4 [:tickets "id-1" :status])))
        (is (nil? (d/current-called s4 :two)))
        (is (not (:ok (d/seat s4 :two "id-1" now))))

        (let [r5 (d/cancel s4 :two "id-2")
              s5 (:state r5)]
          (is (:ok r5))
          (is (= :cancelled (get-in s5 [:tickets "id-2" :status])))
          (is (not (:ok (d/call-next s5 :two now)))))))))

(deftest queue-open-close
  (let [r1 (d/close-queue {} :two)
        s1 (:state r1)]
    (is (= :closed (get-in s1 [:queues :two :status])))
    (is (not (:ok (d/create-ticket s1 2 "remote" now "tok1" "id-1"))))
    (let [r2 (d/open-queue s1 :two)
          s2 (:state r2)]
      (is (= :open (get-in s2 [:queues :two :status])))
      (is (:ok (d/create-ticket s2 2 "remote" now "tok1" "id-1"))))
    (let [r3 (d/close-queue s1 :two)]
      (is (:ok r3))
      (is (= :closed (get-in (:state r3) [:queues :two :status]))))))

(deftest diff-events
  (let [s0 {}
        r1 (d/create-ticket s0 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/call-next s1 :two now)
        s2 (:state r2)
        evts (d/diff-events s1 s2)]
    (is (= 1 (count evts)))
    (is (= :ticket (:kind (first evts))))
    (is (= :called (:type (first evts))))
    (is (= "id-1" (:ticket-id (first evts))))
    (is (= "tok1" (:token (first evts))))))

(deftest ticket-by-token-and-view
  (let [r (d/create-ticket {} 2 "remote" now "tok-x" "id-x")
        s (:state r)]
    (is (= "id-x" (:id (d/ticket-by-token s "tok-x"))))
    (is (nil? (d/ticket-by-token s "nope")))
    (is (= :waiting (:status (d/ticket-status-view s "tok-x"))))))
