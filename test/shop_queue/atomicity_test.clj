(ns shop-queue.atomicity-test
  "Counters and tokens stay unique and continuous across close/open and process
  restart. These tests drive the real API handler (api -> domain -> storage ->
  SQLite) without booting a socket, and exercise the atomic storage primitives
  directly under concurrency."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
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

(deftest append-operation-is-atomic-under-concurrency
  (let [db-path (temp-path "shop-queue-ops")
        ds (storage/init! (storage/connect db-path))
        n 20]
    (try
      (let [results (->> (range n)
                         (mapv (fn [i]
                                 (future (storage/append-operation! ds "store-1"
                                                                    {:action :test :i i}))))
                         (mapv #(:seq (deref %))))]
        (is (= n (count results)))
        (is (= n (count (set results))) "concurrent appends are distinct")
        (is (= (set (range n)) (set results)) "no gap in the sequence"))
      (finally
        (io/delete-file db-path true)))))

(deftest create-ticket-rolls-back-counter-and-token-on-failure
  (let [db-path (temp-path "shop-queue-create-tx")
        system (core/start-system! db-path)]
    (try
      (let [db (:db system)
            before (get-in @(:state system) [:stores "store-1"])]
        (with-redefs [storage/append-operation! (fn [& _] (throw (ex-info "boom" {})))]
          (let [resp ((api/handler system)
                      {:request-method :post
                       :uri "/stores/store-1/tickets"
                       :body (json/generate-string {:party-size 2 :source "remote"})})]
            (is (= 500 (:status resp)))))
        (testing "counter, token and store document are all rolled back together"
          (is (empty? (jdbc/execute! db ["SELECT * FROM counters"]
                                     {:builder-fn rs/as-unqualified-kebab-maps})))
          (is (empty? (jdbc/execute! db ["SELECT * FROM tokens"]
                                     {:builder-fn rs/as-unqualified-kebab-maps})))
          (is (empty? (get-in (storage/load-all db) ["store-1" :tickets])))
          (is (= 0 (get-in (storage/load-all db) ["store-1" :queues :two :counter]))))
        (testing "in-memory state is unchanged"
          (is (= before (get-in @(:state system) [:stores "store-1"])))))
      (finally
        (io/delete-file db-path true)))))

(deftest save-store-detects-version-conflict
  (let [db-path (temp-path "shop-queue-cas")
        ds (storage/init! (storage/connect db-path))]
    (try
      (let [v1 (:version (storage/save-store! ds "store-1" (domain/seed-store {})))]
        (is (= 1 v1))
        (testing "a stale writer still on version 0 is rejected"
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"store version conflict"
                                (storage/save-store! ds "store-1" (domain/seed-store {})))))
        (testing "a writer carrying the current version advances to 2"
          (is (= 2 (:version (storage/save-store! ds "store-1"
                                                   (assoc (domain/seed-store {}) :version 1)))))))
      (finally
        (io/delete-file db-path true)))))

(deftest create-ticket-returns-409-on-store-version-conflict
  (let [db-path (temp-path "shop-queue-create-conflict")
        system (core/start-system! db-path)]
    (try
      ;; Simulate a stale reader: rewind the in-memory version so the CAS fails
      ;; against the already-persisted version.
      (swap! (:state system) update-in [:stores "store-1" :version] dec)
      (let [resp ((api/handler system)
                  {:request-method :post
                   :uri "/stores/store-1/tickets"
                   :body (json/generate-string {:party-size 2 :source "remote"})})]
        (is (= 409 (:status resp)))
        (is (= "store-conflict" (:error (json/parse-string (:body resp) true)))))
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

(deftest timestamps-are-epoch-millis
  (let [db-path (temp-path "shop-queue-epoch")]
    (try
      (let [system (core/start-system! db-path)
            before (System/currentTimeMillis)
            r (post-ticket system "store-1" {:party-size 2 :source "remote"})
            after (System/currentTimeMillis)
            created-at (get-in (storage/load-all (:db system))
                               ["store-1" :tickets (:id (:body r)) :created-at])]
        (is (= 201 (:status r)))
        (is (integer? created-at))
        (is (<= before created-at after))
        (testing "created-at 是 epoch 毫秒（约 13 位）"
          (is (<= 1000000000000 created-at))))
      (finally
        (io/delete-file db-path true)))))
