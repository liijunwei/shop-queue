(ns shop-queue.integration-test
  "End-to-end HTTP test of the full store-queue flow: merchant opens the store
  and starts the queue, a customer takes a number, the merchant calls and seats
  them, and the closed queue rejects new tickets. It boots a real http-kit
  server + client against a temp SQLite file, so api -> domain -> storage ->
  SQLite are all exercised in one place."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [org.httpkit.client :as client]
            [org.httpkit.server :as http]
            [shop-queue.api :as api]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage]))

(def ^:dynamic *server* nil)
(def ^:dynamic *port* nil)
(def ^:dynamic *db* nil)
(def ^:dynamic *db-path* nil)

(defn- url [& segs]
  (apply str "http://127.0.0.1:" *port* (map #(str "/" %) segs)))

;; http-kit's client functions are async and return a promise; deref and parse
;; the JSON body so call sites only deal with {:status .. :body ..}.
(defn- req [promise]
  (let [r @promise]
    (if (:error r)
      r
      (assoc r :body (json/parse-string (:body r) true)))))

(defn- get* [& segs]
  (req (client/get (apply url segs))))

(defn- post* [path & [opts]]
  (req (client/post path opts)))

(defn- post-json [path body]
  (post* path {:headers {"Content-Type" "application/json"}
               :body (json/generate-string body)}))

(defn with-test-server [f]
  (let [db-path (str (io/file (System/getProperty "java.io.tmpdir")
                              (str "shop-queue-it-" (java.util.UUID/randomUUID) ".db")))
        ds (storage/init! (storage/connect db-path))
        state (atom {:stores {"store-1" (domain/seed-store {})}})
        subs (atom {:by-token {} :by-table-type {}})
        system {:db ds :state state :subs subs}
        server (http/run-server (api/handler system) {:port 0 :legacy-return-value? false})]
    (binding [*server* server
              *port* (http/server-port server)
              *db* ds
              *db-path* db-path]
      (try
        (f)
        (finally
          (some-> (http/server-stop! server) deref)
          (io/delete-file db-path true))))))

(use-fixtures :each with-test-server)

(deftest full-store-queue-flow
  (let [token (atom nil)]
    (testing "商家开张：store-1 启动即就绪，三条桌型队列默认开启"
      (let [b (:body (get*))]
        (is (= #{:two :three :large}
               (set (keys (get-in b [:stores :store-1 :queues])))))
        (is (= "open" (get-in b [:stores :store-1 :queues :two :status])))))

    (testing "开启排队：two 队列关→开，最终开启且计数归零"
      (let [closed (:body (post* (url "stores" "store-1" "queues" "two" "close")))
            opened (:body (post* (url "stores" "store-1" "queues" "two" "open")))
            qview  (:body (get* "stores" "store-1" "queues" "two"))]
        (is (= "closed" (:status closed)))
        (is (= "open" (:status opened)))
        (is (= "open" (:status qview)))
        (is (= 0 (:counter qview)))))

    (testing "顾客取号：2 人远程取号 → A001，前方 0 桌"
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 201 (:status r)))
        (is (= "A001" (:id b)))
        (is (= "two" (:table-type b)))
        (is (= 0 (:parties-ahead b)))
        (reset! token (:token b))))

    (testing "顾客凭 token 轮询状态 → waiting"
      (let [b (:body (get* "stores" "store-1" "tickets" @token))]
        (is (= "A001" (:id b)))
        (is (= "waiting" (:status b)))))

    (testing "商家叫号 → A001"
      (let [r (post* (url "stores" "store-1" "queues" "two" "call"))
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= "A001" (:id b)))))

    (testing "顾客入座（吃饭）→ seated"
      (let [r (post* (url "stores" "store-1" "queues" "two" "tickets" "A001" "seat"))
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= "A001" (:id b)))))

    (testing "入座后再次查询状态 → seated"
      (let [b (:body (get* "stores" "store-1" "tickets" @token))]
        (is (= "seated" (:status b)))))

    (testing "关队列后取号返回 409 queue-closed"
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "close")))))
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 409 (:status r)))
        (is (= "queue-closed" (:error b)))))

    (testing "重启后状态保留：从 SQLite 重载，A001 仍为 seated、计数为 1"
      (let [loaded (storage/load-all *db*)]
        (is (= :seated (get-in loaded ["store-1" :tickets "A001" :status])))
        (is (= 1 (get-in loaded ["store-1" :queues :two :counter])))))))
