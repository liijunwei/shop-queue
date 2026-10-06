(ns shop-queue.domain-test
  (:require [clojure.test :refer [deftest is testing]]
            [shop-queue.domain :as d]))

(def now 1700000000)

(deftest table-type-matching
  (testing "1 person is seated at the smallest (2-person) table"
    (is (= :two (d/match-table-type d/default-table-types 1))))
  (testing "a party exactly equal to :max matches that table"
    (is (= :two (d/match-table-type d/default-table-types 2)))
    (is (= :three (d/match-table-type d/default-table-types 3))))
  (testing "a party over every :min lands on large (multi-person)"
    (is (= :large (d/match-table-type d/default-table-types 4)))
    (is (= :large (d/match-table-type d/default-table-types 20))))
  (testing ":max nil means no upper bound"
    (is (= :large (d/match-table-type d/default-table-types 20))))
  (testing "candidates are ordered by :min ascending, not by :max"
    ;; :small has a larger capacity than :medium but a smaller :min; a party of
    ;; 3 fits both, so the :min ordering must pick :small.
    (let [custom {:small  {:label "小桌" :min 1 :max 10}
                  :medium {:label "中桌" :min 2 :max 3}
                  :large  {:label "多人" :min 4 :max nil}}]
      (is (= :small (d/match-table-type custom 3)))))
  (testing "no table type can seat the party"
    (is (nil? (d/match-table-type (dissoc d/default-table-types :large) 10)))))

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
  (is (not (:ok (d/create-ticket {} -1 "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 2.5 "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} "2" "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} nil "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 51 "remote" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 2 "bogus" now "t" "id"))))
  (is (not (:ok (d/create-ticket {} 100 "remote" now "t" "id")))))

(deftest create-ticket-idempotency
  (let [r1 (d/create-ticket {} 2 "scan" now "tok1" "id-1" "scan-key")
        s1 (:state r1)]
    (is (:ok r1))
    (is (= "id-1" (:id (:value r1))))
    (is (nil? (:duplicate (:value r1))))
    (is (= "id-1" (get-in s1 [:idempotency "scan-key"])))
    (testing "same key returns the existing ticket and leaves state untouched"
      (let [r2 (d/create-ticket s1 2 "scan" now "tok2" "id-2" "scan-key")
            s2 (:state r2)]
        (is (:ok r2))
        (is (= "id-1" (:id (:value r2))))
        (is (= "tok1" (:token (:value r2))))
        (is (true? (:duplicate (:value r2))))
        (is (= s1 s2))
        (is (= 1 (get-in s2 [:queues :two :counter]))))))
  (testing "different keys still create distinct tickets"
    (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1" "k1")
          r2 (d/create-ticket (:state r1) 2 "remote" now "tok2" "id-2" "k2")]
      (is (= "id-1" (:id (:value r1))))
      (is (= "id-2" (:id (:value r2))))))
  (testing "no key means no dedup"
    (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
          r2 (d/create-ticket (:state r1) 2 "remote" now "tok2" "id-2")]
      (is (= "id-1" (:id (:value r1))))
      (is (= "id-2" (:id (:value r2)))))))

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

(deftest call-next-auto-no-show
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "remote" now "tok2" "id-2")
        s2 (:state r2)
        r3 (d/call-next s2 :two now)
        s3 (:state r3)]
    (is (= "id-1" (:id (:value r3))))
    (let [r4 (d/call-next s3 :two now)
          s4 (:state r4)]
      (is (:ok r4))
      (is (= "id-2" (:id (:value r4))))
      (is (= :no-show (get-in s4 [:tickets "id-1" :status])))
      (is (= :called (get-in s4 [:tickets "id-2" :status])))
      (is (= "id-2" (:id (d/current-called s4 :two))))
      (testing "at most one called ticket per queue at any time"
        (is (= 1 (count (filter #(= :called (:status %)) (vals (:tickets s4))))))))))

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

(deftest drain-queue
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "scan" now "tok2" "id-2")
        s2 (:state r2)
        r3 (d/create-ticket s2 3 "remote" now "tok3" "id-3")
        s3 (:state r3)
        r4 (d/call-next s3 :two now)
        s4 (:state r4)
        r5 (d/drain-queue s4 :two)
        s5 (:state r5)]
    (is (= "id-1" (:id (:value r4))))
    (is (:ok r5))
    (is (= :drained (get-in s5 [:queues :two :status])))
    (is (= ["id-2"] (:cancelled (:value r5))))
    (is (= :cancelled (get-in s5 [:tickets "id-2" :status])))
    (is (= :called (get-in s5 [:tickets "id-1" :status])))
    (is (= :waiting (get-in s5 [:tickets "id-3" :status])))
    (is (empty? (d/waiting-tickets s5 :two)))
    (testing "drained queue rejects new tickets and further calls"
      (is (not (:ok (d/create-ticket s5 2 "remote" now "tok4" "id-4"))))
      (is (not (:ok (d/call-next s5 :two now)))))
    (testing "the in-flight (called) ticket can still be seated"
      (let [r6 (d/seat s5 :two "id-1" now)]
        (is (:ok r6))
        (is (= :seated (get-in (:state r6) [:tickets "id-1" :status])))))
    (testing "draining an already-drained queue is idempotent"
      (let [r7 (d/drain-queue s5 :two)]
        (is (:ok r7))
        (is (= [] (:cancelled (:value r7))))))))

(deftest drain-events
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "remote" now "tok2" "id-2")
        s2 (:state r2)
        r3 (d/drain-queue s2 :two)
        s3 (:state r3)
        evts (d/diff-events s2 s3)]
    (is (= #{:queue-drained :cancelled} (set (map :type evts))))
    (is (= 1 (count (filter #(= :queue (:kind %)) evts))))
    (is (= ["id-1" "id-2"] (mapv :ticket-id (filter #(= :ticket (:kind %)) evts))))))

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

(deftest token-is-separate-from-display-number
  (let [r (d/create-ticket {} 2 "remote" now "tok-uuid" "id-uuid")
        t (get-in (:state r) [:tickets "id-uuid"])]
    (is (= "tok-uuid" (:token t)))
    (is (= 1 (:n t)))
    (testing "token 与展示号（顺序计数 :n）、内部 id 都分离"
      (is (not= (:token t) (:n t)))
      (is (not= (:token t) (:id t))))))

(deftest requeue-threshold-config
  (testing "seeded store gets the default threshold"
    (is (= d/default-requeue-threshold (get (d/seed-store {}) :requeue-threshold))))
  (testing "set-requeue-threshold validates and stores the value"
    (is (= 5 (get-in (d/set-requeue-threshold {} 5) [:state :requeue-threshold])))
    (is (= 0 (get-in (d/set-requeue-threshold {} 0) [:state :requeue-threshold])))
    (is (not (:ok (d/set-requeue-threshold {} -1))))
    (is (not (:ok (d/set-requeue-threshold {} 2.5))))
    (is (not (:ok (d/set-requeue-threshold {} "5"))))
    (is (= :invalid-threshold (get-in (d/set-requeue-threshold {} -1) [:error :code])))))

(deftest requeue-no-show-ticket
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "remote" now "tok2" "id-2")
        s2 (:state r2)
        r3 (d/create-ticket s2 2 "remote" now "tok3" "id-3")
        s3 (:state r3)
        r4 (d/create-ticket s3 2 "remote" now "tok4" "id-4")
        s4 (:state r4)
        r5 (d/call-next s4 :two now)
        s5 (:state r5)
        r6 (d/no-show s5 :two "id-1" now)
        s6 (:state r6)
        r7 (d/requeue s6 :two "id-1" now)
        s7 (:state r7)]
    (is (:ok r7))
    (is (= :waiting (get-in s7 [:tickets "id-1" :status])))
    (is (nil? (get-in s7 [:tickets "id-1" :called-at])))
    (is (= 2 (:parties-ahead (:value r7))))
    (testing "向后延两桌：过号票插到第 3 位"
      (is (= ["id-2" "id-3" "id-1" "id-4"] (mapv :id (d/waiting-tickets s7 :two)))))))

(deftest requeue-queue-too-long
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/create-ticket s1 2 "remote" now "tok2" "id-2")
        s2 (:state r2)
        r3 (d/create-ticket s2 2 "remote" now "tok3" "id-3")
        s3 (:state r3)
        r4 (d/call-next s3 :two now)
        s4 (:state r4)
        r5 (d/no-show s4 :two "id-1" now)
        s5 (:state r5)
        r6 (d/set-requeue-threshold s5 1)
        s6 (:state r6)
        r7 (d/requeue s6 :two "id-1" now)]
    (is (not (:ok r7)))
    (is (= :queue-too-long (get-in r7 [:error :code])))
    (is (= :no-show (get-in s6 [:tickets "id-1" :status])))))

(deftest requeue-invalid-transitions
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)]
    (testing "仍 waiting 的票不能 requeue"
      (let [r (d/requeue s1 :two "id-1" now)]
        (is (not (:ok r)))
        (is (= :invalid-transition (get-in r [:error :code])))))
    (testing "不存在的票返回 not-found"
      (is (= :not-found (get-in (d/requeue s1 :two "nope" now) [:error :code]))))
    (testing "桌型不匹配返回 not-found"
      (is (= :not-found (get-in (d/requeue s1 :three "id-1" now) [:error :code]))))))

(deftest requeue-rejects-drained-queue
  (let [r1 (d/create-ticket {} 2 "remote" now "tok1" "id-1")
        s1 (:state r1)
        r2 (d/call-next s1 :two now)
        s2 (:state r2)
        r3 (d/no-show s2 :two "id-1" now)
        s3 (:state r3)
        r4 (d/drain-queue s3 :two)
        s4 (:state r4)
        r5 (d/requeue s4 :two "id-1" now)]
    (is (not (:ok r5)))
    (is (= :queue-drained (get-in r5 [:error :code])))))

(deftest query-operations
  (let [ops [{:seq 0 :ts 100 :actor :customer :action :create-ticket :target {:kind :ticket :id "a"}}
             {:seq 1 :ts 200 :actor :staff :action :call :target {:kind :ticket :id "a"}}
             {:seq 2 :ts 300 :actor :staff :action :seat :target {:kind :ticket :id "a"}}
             {:seq 3 :ts 400 :actor :customer :action :cancel :target {:kind :ticket :id "b"}}]]
    (testing "no filters returns everything"
      (is (= 4 (count (d/query-operations ops {})))))
    (testing "actor filter"
      (is (= [0 3] (mapv :seq (d/query-operations ops {:actor :customer})))))
    (testing "action filter"
      (is (= [1] (mapv :seq (d/query-operations ops {:action :call})))))
    (testing "since filter is inclusive"
      (is (= [1 2 3] (mapv :seq (d/query-operations ops {:since 200})))))
    (testing "combined filters"
      (is (= [3] (mapv :seq (d/query-operations ops {:actor :customer :action :cancel})))))))
