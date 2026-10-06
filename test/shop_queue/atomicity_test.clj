(ns shop-queue.atomicity-test
  "Counters and tokens stay unique and continuous across close/open and process
  restart. These tests drive the real API handler (api -> domain -> storage ->
  SQLite) without booting a socket, and exercise the atomic storage primitives
  directly under concurrency."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [shop-queue.api :as api]
            [shop-queue.core :as core]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage]))

(defn- temp-path [prefix]
  (str (io/file (System/getProperty "java.io.tmpdir")
                (str prefix "-" (java.util.UUID/randomUUID) ".db"))))

(defn- call [system req]
  (let [resp ((api/handler system) req)]
    (assoc resp :body (json/parse-string (:body resp) true))))

(defn- post-ticket [system store-id body]
  (call system {:request-method :post
                :uri (str "/stores/" store-id "/tickets")
                :body (json/generate-string body)}))

(defn- queue-status [system store-id tt]
  (call system {:request-method :get
                :uri (str "/stores/" store-id "/queues/" (name tt))}))

(defn- queue-action [system store-id tt action]
  (call system {:request-method :post
                :uri (str "/stores/" store-id "/queues/" (name tt) "/" (name action))}))

(deftest next-counter-is-atomic-under-concurrency
  (let [db-path (temp-path "shop-queue-atomic")
        ds (storage/init! (storage/connect db-path))
        n 30]
    (try
      (let [results (->> (range n)
                         (mapv (fn [_] (future (storage/next-counter! ds "store-1" :two 0))))
                         (mapv deref))]
        (is (= n (count results)))
        (is (= n (count (set results))) "concurrent allocations are distinct")
        (is (= (set (range 1 (inc n))) (set results)) "no gap in the sequence"))
      (finally
        (io/delete-file db-path true)))))

(deftest next-counter-continues-from-persisted-document-counter
  (let [db-path (temp-path "shop-queue-restart-counter")
        ds (storage/init! (storage/connect db-path))]
    (try
      ;; A store document already at counter 5, with no counters row: this is
      ;; the shape a restart sees when the counters table was not populated.
      (storage/save-store! ds "store-1"
                           (assoc-in (domain/seed-store {}) [:queues :two :counter] 5))
      (let [current (get-in (storage/load-all ds) ["store-1" :queues :two :counter])]
        (is (= 5 current))
        (is (= 6 (storage/next-counter! ds "store-1" :two current))))
      (finally
        (io/delete-file db-path true)))))

(deftest token-claim-enforces-global-uniqueness
  (let [db-path (temp-path "shop-queue-token")
        ds (storage/init! (storage/connect db-path))]
    (try
      (is (true? (storage/claim-token! ds "store-1" "tok-dup")))
      (is (false? (storage/claim-token! ds "store-1" "tok-dup")))
      (is (false? (storage/claim-token! ds "store-2" "tok-dup"))
          "token uniqueness is global, not just per store")
      (finally
        (io/delete-file db-path true)))))

(deftest close-then-open-keeps-counter-continuous
  (let [db-path (temp-path "shop-queue-reopen")]
    (try
      (let [system (core/start-system! db-path)]
        (is (= 201 (:status (post-ticket system "store-1" {:party-size 2 :source "remote"}))))
        (is (= 201 (:status (post-ticket system "store-1" {:party-size 2 :source "remote"}))))
        (is (= 200 (:status (queue-action system "store-1" :two :close))))
        (is (= 200 (:status (queue-action system "store-1" :two :open))))
        (is (= 201 (:status (post-ticket system "store-1" {:party-size 2 :source "remote"}))))
        (is (= 3 (:counter (:body (queue-status system "store-1" :two))))))
      (finally
        (io/delete-file db-path true)))))

(deftest restart-does-not-repeat-numbers
  (let [db-path (temp-path "shop-queue-restart-numbers")]
    (try
      (let [system1 (core/start-system! db-path)
            r1 (post-ticket system1 "store-1" {:party-size 2 :source "remote"})
            r2 (post-ticket system1 "store-1" {:party-size 2 :source "remote"})]
        (is (= 201 (:status r1)))
        (is (= 201 (:status r2)))
        (is (not= (:token (:body r1)) (:token (:body r2))))
        ;; A second process boots from the same SQLite file.
        (let [system2 (core/start-system! db-path)
              r3 (post-ticket system2 "store-1" {:party-size 2 :source "remote"})]
          (is (= 201 (:status r3)))
          (is (not= (:token (:body r3)) (:token (:body r1))))
          (is (not= (:token (:body r3)) (:token (:body r2))))
          (is (= 3 (:counter (:body (queue-status system2 "store-1" :two)))))))
      (finally
        (io/delete-file db-path true)))))

(deftest timestamps-are-epoch-seconds
  (let [db-path (temp-path "shop-queue-epoch")]
    (try
      (let [system (core/start-system! db-path)
            before (quot (System/currentTimeMillis) 1000)
            r (post-ticket system "store-1" {:party-size 2 :source "remote"})
            after (quot (System/currentTimeMillis) 1000)
            created-at (get-in (storage/load-all (:db system))
                               ["store-1" :tickets (:id (:body r)) :created-at])]
        (is (= 201 (:status r)))
        (is (integer? created-at))
        (is (<= before created-at after))
        (testing "created-at 是 epoch 秒（约 10 位），不是毫秒"
          (is (<= 1000000000 created-at))))
      (finally
        (io/delete-file db-path true)))))
