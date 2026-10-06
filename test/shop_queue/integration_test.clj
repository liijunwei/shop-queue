(ns shop-queue.integration-test
  "End-to-end HTTP test of the full store-queue flow: merchant opens the store
  and starts the queue, a customer takes a number, the merchant calls and seats
  them, and the closed queue rejects new tickets. It boots a real http-kit
  server + client against a temp SQLite file, so api -> domain -> storage ->
  SQLite are all exercised in one place."
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [next.jdbc :as jdbc]
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

(defn- query-string [params]
  (when (seq params)
    (str "?" (str/join "&"
                       (map (fn [[k v]] (str (name k) "=" v)) params)))))

(defn- get-q* [qparams & segs]
  (req (client/get (str (apply url segs) (query-string qparams)))))

(defn- post* [path & [opts]]
  (req (client/post path opts)))

(defn- post-json [path body]
  (post* path {:headers {"Content-Type" "application/json"}
               :body (json/generate-string body)}))

(defn- uuid-string? [s]
  (boolean (try (java.util.UUID/fromString s) (catch Exception _ nil))))

(defn with-test-server [f]
  (let [db-path (str (io/file (System/getProperty "java.io.tmpdir")
                              (str "shop-queue-it-" (java.util.UUID/randomUUID) ".db")))
        ds (storage/init! (storage/connect db-path))
        state (atom {:stores {"store-1" (domain/seed-store {})
                              "store-2" (domain/seed-store {})}})
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
  (let [token (atom nil)
        ticket-id (atom nil)]
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

    (testing "顾客取号：2 人远程取号 → UUID，前方 0 桌"
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 201 (:status r)))
        (is (uuid-string? (:id b)))
        (is (= "two" (:table-type b)))
        (is (= 0 (:parties-ahead b)))
        (reset! token (:token b))
        (reset! ticket-id (:id b))))

    (testing "顾客凭 token 轮询状态 → waiting"
      (let [b (:body (get* "stores" "store-1" "tickets" @token))]
        (is (= @ticket-id (:id b)))
        (is (= "waiting" (:status b)))))

    (testing "商家叫号 → 当前票"
      (let [r (post* (url "stores" "store-1" "queues" "two" "call"))
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= @ticket-id (:id b)))))

    (testing "顾客入座（吃饭）→ seated"
      (let [r (post* (url "stores" "store-1" "queues" "two" "tickets" @ticket-id "seat"))
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= @ticket-id (:id b)))))

    (testing "入座后再次查询状态 → seated"
      (let [b (:body (get* "stores" "store-1" "tickets" @token))]
        (is (= "seated" (:status b)))))

    (testing "关队列后取号返回 409 queue-closed"
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "close")))))
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 409 (:status r)))
        (is (= "queue-closed" (:error b)))))

    (testing "重启后状态保留：从 SQLite 重载，票仍为 seated、计数为 1"
      (let [loaded (storage/load-all *db*)]
        (is (= :seated (get-in loaded ["store-1" :tickets @ticket-id :status])))
        (is (= 1 (get-in loaded ["store-1" :queues :two :counter])))))))

(deftest party-size-validation
  (testing "0, negative, non-integer, missing and wrong-typed party-size are all 400"
    (doseq [body [{:party-size 0}
                  {:party-size -1}
                  {:party-size 2.5}
                  {:party-size "2"}
                  {:source "remote"}]]
      (let [r (post-json (url "stores" "store-1" "tickets") body)
            b (:body r)]
        (is (= 400 (:status r)))
        (is (= "invalid-party-size" (:error b)))))))

(deftest closed-queue-preserves-existing-tickets
  (let [r1 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b1 (:body r1)
        id (:id b1)
        token (:token b1)]
    (is (= 201 (:status r1)))
    (testing "an already-issued ticket still calls and seats after the queue closes"
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "close")))))
      (is (= id (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "tickets" id "seat")))))
      (is (= "seated" (:status (:body (get* "stores" "store-1" "tickets" token))))))
    (testing "a brand-new ticket is still rejected with 409 queue-closed"
      (let [r2 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b2 (:body r2)]
        (is (= 409 (:status r2)))
        (is (= "queue-closed" (:error b2)))))))

(deftest customer-self-cancel
  (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b (:body r)
        token (:token b)
        id (:id b)]
    (is (= 201 (:status r)))
    (testing "顾客凭 token 取消自己的号，票状态变为 cancelled"
      (let [rc (post* (url "stores" "store-1" "tickets" token "cancel"))
            bc (:body rc)]
        (is (= 200 (:status rc)))
        (is (= id (:id bc)))
        (is (= "cancelled" (:status (:body (get* "stores" "store-1" "tickets" token)))))))
    (testing "重复取消同一张非 waiting 票返回 409 invalid-transition"
      (let [rc (post* (url "stores" "store-1" "tickets" token "cancel"))
            bc (:body rc)]
        (is (= 409 (:status rc)))
        (is (= "invalid-transition" (:error bc)))))
    (testing "不存在的 token 返回 404 not-found"
      (let [rc (post* (url "stores" "store-1" "tickets" "no-such-token" "cancel"))
            bc (:body rc)]
        (is (= 404 (:status rc)))
        (is (= "not-found" (:error bc)))))))

(deftest idempotent-ticket-creation
  (testing "same idempotency-key returns the same ticket and does not advance the counter"
    (let [body {:party-size 2 :source "scan" :idempotency-key "qr-abc"}
          r1 (post-json (url "stores" "store-1" "tickets") body)
          b1 (:body r1)
          r2 (post-json (url "stores" "store-1" "tickets") body)
          b2 (:body r2)]
      (is (= 201 (:status r1)))
      (is (= 200 (:status r2)))
      (is (= (:id b1) (:id b2)))
      (is (= (:token b1) (:token b2)))
      (is (true? (:duplicate b2))))
    (testing "counter stayed at 1"
      (is (= 1 (:counter (:body (get* "stores" "store-1" "queues" "two")))))))
  (testing "replay does not append a second create-ticket operation"
    (let [ops (:operations (:body (get* "stores" "store-1" "operations")))]
      (is (= ["create-ticket"] (mapv :action ops))))))

(deftest distinct-idempotency-keys-create-distinct-tickets
  (let [r1 (post-json (url "stores" "store-1" "tickets")
                      {:party-size 2 :source "remote" :idempotency-key "k1"})
        r2 (post-json (url "stores" "store-1" "tickets")
                      {:party-size 2 :source "remote" :idempotency-key "k2"})]
    (is (= 201 (:status r1)))
    (is (= 201 (:status r2)))
    (is (not= (:id (:body r1)) (:id (:body r2))))))

(deftest drain-queue-flow
  (let [r1 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b1 (:body r1)
        id1 (:id b1)
        tok1 (:token b1)
        r2 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b2 (:body r2)
        id2 (:id b2)
        tok2 (:token b2)]
    (is (= 201 (:status r1)))
    (is (= 201 (:status r2)))
    (testing "叫号一张，随后 drain 把剩余 waiting 全部 cancelled"
      (is (= id1 (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
      (let [rd (:body (post* (url "stores" "store-1" "queues" "two" "drain")))]
        (is (= "drained" (:status rd)))
        (is (= [id2] (:cancelled rd)))))
    (testing "drain 后：waiting 变 cancelled，called 保留，新号/叫号均被拒"
      (is (= "cancelled" (:status (:body (get* "stores" "store-1" "tickets" tok2)))))
      (is (= "called" (:status (:body (get* "stores" "store-1" "tickets" tok1)))))
      (let [r3 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})]
        (is (= 409 (:status r3)))
        (is (= "queue-closed" (:error (:body r3)))))
      (let [r4 (post* (url "stores" "store-1" "queues" "two" "call"))]
        (is (= 409 (:status r4)))
        (is (= "queue-drained" (:error (:body r4))))))
    (testing "在途 called 票仍可入座"
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "tickets" id1 "seat")))))
      (is (= "seated" (:status (:body (get* "stores" "store-1" "tickets" tok1))))))))

(deftest operation-records-flow
  (let [t1 (atom nil)
        t2 (atom nil)]
    (testing "顾客取号 + 店员操作 + 顾客取消，均落入操作记录"
      (let [r1 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b1 (:body r1)]
        (is (= 201 (:status r1)))
        (reset! t1 {:token (:token b1) :id (:id b1)})
        (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "close")))))
        (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "open")))))
        (is (= (:id @t1) (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
        (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "tickets" (:id @t1) "seat")))))
        (let [r2 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
              b2 (:body r2)]
          (is (= 201 (:status r2)))
          (reset! t2 {:token (:token b2) :id (:id b2)})
          (is (= 200 (:status (post* (url "stores" "store-1" "tickets" (:token @t2) "cancel"))))))))

    (testing "操作列表：按发生顺序 append-only"
      (let [r (get* "stores" "store-1" "operations")
            b (:body r)
            ops (:operations b)]
        (is (= 200 (:status r)))
        (is (= 7 (:total b)))
        (is (= ["create-ticket" "close-queue" "open-queue" "call" "seat"
                "create-ticket" "cancel"]
               (mapv :action ops)))
        (is (= ["customer" "staff" "staff" "staff" "staff" "customer" "customer"]
               (mapv :actor ops)))
        (is (= [0 1 2 3 4 5 6] (mapv :seq ops)))
        (is (= (:id @t1) (get-in ops [3 :target :id])))))

    (testing "按 actor 过滤"
      (let [b (:body (get-q* {:actor "customer"} "stores" "store-1" "operations"))]
        (is (= 3 (:total b)))
        (is (every? #(= "customer" (:actor %)) (:operations b)))))

    (testing "按 action 过滤"
      (let [b (:body (get-q* {:action "seat"} "stores" "store-1" "operations"))]
        (is (= 1 (:total b)))
        (is (= "seat" (:action (first (:operations b)))))))

    (testing "分页 limit/offset"
      (let [p1 (:body (get-q* {:limit "2" :offset "0"} "stores" "store-1" "operations"))
            p2 (:body (get-q* {:limit "2" :offset "6"} "stores" "store-1" "operations"))]
        (is (= 2 (count (:operations p1))))
        (is (= 7 (:total p1)))
        (is (= 1 (count (:operations p2))))
        (is (= "cancel" (:action (first (:operations p2)))))))

    (testing "单号时间线"
      (let [tl1 (:body (get* "stores" "store-1" "tickets" (:token @t1) "operations"))
            tl2 (:body (get* "stores" "store-1" "tickets" (:token @t2) "operations"))]
        (is (= (:id @t1) (:ticket-id tl1)))
        (is (= ["create-ticket" "call" "seat"] (mapv :action (:operations tl1))))
        (is (= (:id @t2) (:ticket-id tl2)))
        (is (= ["create-ticket" "cancel"] (mapv :action (:operations tl2))))))

    (testing "未知 ticket 时间线返回 404"
      (is (= 404 (:status (get* "stores" "store-1" "tickets" "no-such-token" "operations")))))))

(deftest call-auto-no-show
  (let [r1 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b1 (:body r1)
        r2 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b2 (:body r2)]
    (is (= 201 (:status r1)))
    (is (= 201 (:status r2)))
    (testing "第一次叫号叫到第一张"
      (is (= (:id b1) (:id (:body (post* (url "stores" "store-1" "queues" "two" "call")))))))
    (testing "再次叫号把上一张自动过号，并叫到下一张"
      (is (= (:id b2) (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
      (is (= "no-show" (:status (:body (get* "stores" "store-1" "tickets" (:token b1))))))
      (is (= "called" (:status (:body (get* "stores" "store-1" "tickets" (:token b2)))))))))

(deftest requeue-no-show-ticket-flow
  (let [ids (atom [])
        tokens (atom [])]
    (dotimes [_ 4]
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 201 (:status r)))
        (swap! ids conj (:id b))
        (swap! tokens conj (:token b))))
    (testing "叫号第一张并过号"
      (is (= (first @ids) (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "tickets" (first @ids) "no-show"))))))
    (testing "过号票 requeue → 回到 waiting，前面正好两桌"
      (let [r (post* (url "stores" "store-1" "queues" "two" "tickets" (first @ids) "requeue"))
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= (first @ids) (:id b)))
        (is (= 2 (:parties-ahead b))))
      (let [b (:body (get* "stores" "store-1" "tickets" (first @tokens)))]
        (is (= "waiting" (:status b)))
        (is (= 2 (:parties-ahead b)))))
    (testing "等待顺序：第二、三张在前，过号票第三，第四张最后"
      (let [b (:body (get* "stores" "store-1" "queues" "two"))]
        (is (= [(second @ids) (nth @ids 2) (first @ids) (nth @ids 3)]
               (:waiting b)))))))

(deftest requeue-queue-too-long-flow
  (let [ids (atom [])
        tokens (atom [])]
    (testing "可配置阈值：调成 1"
      (let [r (post-json (url "stores" "store-1" "requeue-threshold") {:threshold 1})
            b (:body r)]
        (is (= 200 (:status r)))
        (is (= 1 (:requeue-threshold b)))))
    (dotimes [_ 3]
      (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
            b (:body r)]
        (is (= 201 (:status r)))
        (swap! ids conj (:id b))
        (swap! tokens conj (:token b))))
    (testing "叫号第一张并过号"
      (is (= (first @ids) (:id (:body (post* (url "stores" "store-1" "queues" "two" "call"))))))
      (is (= 200 (:status (post* (url "stores" "store-1" "queues" "two" "tickets" (first @ids) "no-show"))))))
    (testing "队列长度 2 超过阈值 1，requeue 被拒 → 需重新取号"
      (let [r (post* (url "stores" "store-1" "queues" "two" "tickets" (first @ids) "requeue"))
            b (:body r)]
        (is (= 409 (:status r)))
        (is (= "queue-too-long" (:error b)))))
    (testing "过号票仍是 no-show"
      (is (= "no-show" (:status (:body (get* "stores" "store-1" "tickets" (first @tokens)))))))))

(deftest set-requeue-threshold-validation-flow
  (testing "非法阈值返回 400 invalid-threshold"
    (doseq [body [{:threshold -1} {:threshold 2.5} {:threshold "5"} {}]]
      (let [r (post-json (url "stores" "store-1" "requeue-threshold") body)
            b (:body r)]
        (is (= 400 (:status r)))
        (is (= "invalid-threshold" (:error b)))))))

(deftest token-is-unguessable-and-distinct-from-id
  (let [r1 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        r2 (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        b1 (:body r1)
        b2 (:body r2)]
    (is (= 201 (:status r1)))
    (is (= 201 (:status r2)))
    (testing "token 是高熵 UUID，不可枚举/不可猜测"
      (is (uuid-string? (:token b1)))
      (is (uuid-string? (:token b2)))
      (is (not= (:token b1) (:token b2))))
    (testing "token 与内部 id 分离，二者互不相等"
      (is (uuid-string? (:id b1)))
      (is (not= (:token b1) (:id b1)))
      (is (not= (:token b2) (:id b2))))))

(deftest unified-404-for-missing-resources
  (testing "不存在的 store 一律 404，且不会被静默创建"
    (is (= 404 (:status (get* "stores" "store-9" "tickets" "no-such-token"))))
    (is (= 404 (:status (get* "stores" "store-9" "queues" "two"))))
    (is (= 404 (:status (post* (url "stores" "store-9" "queues" "two" "close")))))
    (is (= 404 (:status (post* (url "stores" "store-9" "queues" "two" "call")))))
    (is (= 404 (:status (post* (url "stores" "store-9" "queues" "two" "tickets" "x" "seat")))))
    (is (= 404 (:status (post-json (url "stores" "store-9" "tickets") {:party-size 2 :source "remote"}))))
    (is (= 404 (:status (post-json (url "stores" "store-9" "requeue-threshold") {:threshold 5}))))
    (is (= 404 (:status (get* "stores" "store-9" "operations"))))
    (is (not (contains? (:stores (:body (get*))) "store-9"))))
  (testing "不存在的 tt 返回 404"
    (is (= 404 (:status (get* "stores" "store-1" "queues" "bogus"))))
    (is (= 404 (:status (post* (url "stores" "store-1" "queues" "bogus" "call"))))))
  (testing "不存在的 ticket/token 返回 404"
    (is (= 404 (:status (get* "stores" "store-1" "tickets" "no-such-token"))))
    (is (= 404 (:status (get* "stores" "store-1" "tickets" "no-such-token" "operations"))))
    (is (= 404 (:status (post* (url "stores" "store-1" "tickets" "no-such-token" "cancel")))))))

(deftest ticket-token-does-not-leak-store-membership
  (let [r (post-json (url "stores" "store-1" "tickets") {:party-size 2 :source "remote"})
        token (:token (:body r))]
    (is (= 201 (:status r)))
    (testing "store-2 存在，但 token 属于 store-1：查票 404，且与「token 不存在」响应完全一致"
      (let [wrong-store (get* "stores" "store-2" "tickets" token)
            no-such (get* "stores" "store-2" "tickets" "no-such-token")]
        (is (= 404 (:status wrong-store)))
        (is (= 404 (:status no-such)))
        (is (= (:body wrong-store) (:body no-such)))
        (is (= (:body wrong-store) {:error "not-found" :message "ticket not found"}))))
    (testing "不存在的 store 也返回同样的 404 响应，不泄露 store 是否存在"
      (is (= (:body (get* "stores" "store-9" "tickets" token))
             (:body (get* "stores" "store-9" "tickets" "no-such-token")))))
    (testing "token 在自己所属的 store 里仍能正常查到"
      (is (= 200 (:status (get* "stores" "store-1" "tickets" token)))))))

(deftest healthz-reflects-db-writability
  (testing "可写数据库返回 200 ok/writable"
    (let [r (get* "healthz")
          b (:body r)]
      (is (= 200 (:status r)))
      (is (= {:status "ok" :db "writable"} b))))
  (testing "只读数据库返回 503 unavailable/not-writable"
    (let [ro-ds (jdbc/get-datasource {:jdbcUrl (str "jdbc:sqlite:" *db-path* "?open_mode=1")})
          ro-system {:db ro-ds
                     :state (atom {:stores {"store-1" (domain/seed-store {})}})
                     :subs (atom {})}
          resp ((api/handler ro-system) {:request-method :get :uri "/healthz"})]
      (is (= 503 (:status resp)))
      (is (= {:status "unavailable" :db "not-writable"}
             (json/parse-string (:body resp) true))))))

(deftest request-log-fields-derive-store-and-ticket
  (testing "store-id 与 ticket-id 从 URI 提取"
    (is (= {:method "post"
            :uri "/stores/s1/queues/two/tickets/t1/seat"
            :store-id "s1"
            :ticket-id "t1"}
           (api/request-log-fields
             {:request-method :post :uri "/stores/s1/queues/two/tickets/t1/seat"}))))
  (testing "只含 store-id 时 ticket-id 为 nil"
    (is (= {:method "get" :uri "/stores/s1/tickets" :store-id "s1" :ticket-id nil}
           (api/request-log-fields {:request-method :get :uri "/stores/s1/tickets"}))))
  (testing "非 stores 路由两者均为 nil"
    (is (= {:method "get" :uri "/healthz" :store-id nil :ticket-id nil}
           (api/request-log-fields {:request-method :get :uri "/healthz"})))))

(deftest request-log-line-carries-structured-fields
  (let [system {:db *db*
                :state (atom {:stores {"store-1" (domain/seed-store {})}})
                :subs (atom {})}
        out (with-out-str
              ((api/handler system)
               {:request-method :post
                :uri "/stores/store-1/queues/two/tickets/some-ticket-id/seat"}))
        log (json/parse-string (str/trim out) true)]
    (is (= "request" (:event log)))
    (is (uuid-string? (:request-id log)))
    (is (= "post" (:method log)))
    (is (= "store-1" (:store-id log)))
    (is (= "some-ticket-id" (:ticket-id log)))
    (is (= 404 (:status log)))
    (is (number? (:duration-ms log)))))

(comment (full-store-queue-flow))
