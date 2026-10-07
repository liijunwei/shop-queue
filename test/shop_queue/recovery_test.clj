(ns shop-queue.recovery-test
  "Backup & recovery: VACUUM INTO snapshot, restart consistency, and the
  startup guard that refuses to come online with empty state when load fails."
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing]]
            [next.jdbc :as jdbc]
            [shop-queue.core :as core]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage]))

(defn- temp-path [prefix]
  (str (io/file (System/getProperty "java.io.tmpdir")
                (str prefix "-" (java.util.UUID/randomUUID) ".db"))))

(deftest vacuum-into-backup-round-trips-state
  (let [db-path (temp-path "shop-queue-src")
        backup-path (temp-path "shop-queue-backup")]
    (try
      (let [system (core/start-system! db-path)
            ds (:db system)
            ;; Drive the domain to a non-trivial state and persist it.
            s1 (:state (domain/create-ticket (get-in @(:state system) [:stores "store-1"])
                                             2 "remote"
                                             1700000000 "tok-1" "id-1"))
            s2 (:state (domain/call-next s1 :two 1700000001))
            _ (storage/save-store! ds "store-1" s2)
            out (storage/backup! ds backup-path)]
        (is (= backup-path out))
        (is (.exists (io/file backup-path)))
        (testing "the snapshot is a complete, readable database"
          (let [loaded (storage/load-all (storage/connect backup-path))]
            (is (= :called (get-in loaded ["store-1" :tickets "id-1" :status])))
            (is (= "tok-1" (get-in loaded ["store-1" :tickets "id-1" :token])))
            (is (= 1 (get-in loaded ["store-1" :queues :two :counter]))))))
      (finally
        (io/delete-file db-path true)
        (io/delete-file backup-path true)))))

(deftest restart-reloads-consistent-state
  (let [db-path (temp-path "shop-queue-restart")]
    (try
      (let [system1 (core/start-system! db-path)
            ds1 (:db system1)
            ;; 取号 -> 叫号 -> 入座 -> 再取一张 3 人桌，最后落盘。
            s1 (:state (domain/create-ticket (get-in @(:state system1) [:stores "store-1"])
                                             2 "remote"
                                             1000 "tok-1" "id-1"))
            s2 (:state (domain/call-next s1 :two 1001))
            s3 (:state (domain/seat s2 :two "id-1" 1002))
            s4 (:state (domain/create-ticket s3 3 "scan" 1003 "tok-2" "id-2"))]
        (storage/save-store! ds1 "store-1" s4)
        (testing "断电/重启后从同一文件重载得到与落盘前一致的状态"
          (let [system2 (core/start-system! db-path)
                loaded (get-in @(:state system2) [:stores "store-1"])]
            (is (= :seated (get-in loaded [:tickets "id-1" :status])))
            (is (= :waiting (get-in loaded [:tickets "id-2" :status])))
            (is (= :three (get-in loaded [:tickets "id-2" :table-type])))
            (is (= 1 (get-in loaded [:queues :two :counter])))
            (is (= 1 (get-in loaded [:queues :three :counter]))))))
      (finally (io/delete-file db-path true)))))

(deftest startup-refuses-empty-state-on-load-failure
  (let [db-path (temp-path "shop-queue-corrupt")]
    (try
      (let [ds (storage/init! (storage/connect db-path))]
        (jdbc/execute! ds ["INSERT INTO stores (store_id, data) VALUES (?, ?)"
                           "store-1" "{:tickets"])
        (testing "load 失败时不带空/种子状态静默上线，而是抛出明确错误"
          (is (thrown-with-msg? clojure.lang.ExceptionInfo
                                #"refusing to start with empty state"
                                (core/start-system! db-path)))))
      (finally (io/delete-file db-path true)))))
