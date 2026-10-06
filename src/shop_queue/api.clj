(ns shop-queue.api
  "HTTP layer: a hand-written router over http-kit, plus the SSE subscription
  registry. The domain stays pure; IO (JSON, SQLite, channels) lives here."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [org.httpkit.server :as http]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage]))

(def write-lock (Object.))

(defn- now-ms [] (System/currentTimeMillis))

(defn- json-response [status body]
  {:status status
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (json/generate-string body)})

(defn- error-status [code]
  (case code
    (:invalid-party-size :party-size-too-large :invalid-source :no-matching-table-type) 400
    :not-found 404
    (:queue-closed :empty-queue :ticket-in-flight :invalid-transition) 409
    500))

(defn- domain-error [result]
  (let [{:keys [code message]} (:error result)]
    (json-response (error-status code) {:error (name code) :message message})))

(defn- body-string [req]
  (let [b (:body req)]
    (cond
      (nil? b) ""
      (string? b) b
      (bytes? b) (String. ^bytes b "UTF-8")
      :else (slurp b))))

(defn- parse-json [req]
  (let [s (body-string req)]
    (if (str/blank? s) {} (json/parse-string s true))))

(defn- segments [uri]
  (->> (str/split (or uri "") #"/")
       (remove str/blank?)
       vec))

;; ---- SSE registry (IO state, never inside domain state) ----

(defn- subscribe! [subs kind key ch]
  (swap! subs update-in [kind key] (fnil conj #{}) ch))

(defn- unsubscribe! [subs kind key ch]
  (swap! subs update-in [kind key] (fnil disj #{}) ch))

(defn- sse-frame [event-type data]
  (str "event: " (name event-type) "\n"
       "data: " (json/generate-string data) "\n\n"))

(defn- send-event! [ch event-type data]
  (http/send! ch (sse-frame event-type data) false))

(defn- publish-event! [subs ev]
  (case (:kind ev)
    :ticket (doseq [ch (get-in @subs [:by-token (:token ev)])]
              (send-event! ch (:type ev)
                {:type (:type ev) :ticket-id (:ticket-id ev) :table-type (:table-type ev)}))
    :queue (doseq [ch (get-in @subs [:by-table-type (:table-type ev)])]
             (send-event! ch (:type ev)
               {:type (:type ev) :table-type (:table-type ev)}))
    nil))

;; ---- State access + write path ----

(defn- get-store [system store-id]
  (domain/seed-store (get-in @(:state system) [:stores store-id])))

(defn- transact! [system store-id f]
  (locking write-lock
    (let [old-store (get-store system store-id)
          result (f old-store)]
      (when (:ok result)
        (storage/save-store! (:db system) store-id (:state result))
        (swap! (:state system) assoc-in [:stores store-id] (:state result))
        (doseq [ev (domain/diff-events old-store (:state result))]
          (publish-event! (:subs system) ev)))
      result)))

;; ---- Handlers ----

(defn- home [system]
  (json-response 200
    {:stores (into {}
                   (map (fn [[id s]] [id (domain/store-view s)]))
                   (:stores @(:state system))) }))

(defn- create-ticket! [system store-id req]
  (let [body (parse-json req)
        party-size (:party-size body)
        source (or (:source body) "remote")]
    (if (not (domain/valid-party-size? party-size))
      (json-response 400 {:error "invalid-party-size" :message "party-size must be a positive integer"})
      (let [token (str (java.util.UUID/randomUUID))
            id (str (java.util.UUID/randomUUID))
            result (transact! system store-id
                     (fn [s] (domain/create-ticket s party-size source (now-ms) token id)))]
        (if (:ok result) (json-response 201 (:value result)) (domain-error result))))))

(defn- ticket-status! [system store-id token]
  (if-let [view (domain/ticket-status-view (get-store system store-id) token)]
    (json-response 200 view)
    (json-response 404 {:error "not-found" :message "ticket not found"})))

(defn- cancel-by-token! [system store-id token]
  (if-let [t (domain/ticket-by-token (get-store system store-id) token)]
    (let [result (transact! system store-id
                   (fn [s] (domain/cancel s (:table-type t) (:id t))))]
      (if (:ok result) (json-response 200 (:value result)) (domain-error result)))
    (json-response 404 {:error "not-found" :message "ticket not found"})))

(defn- queue-status! [system store-id tt]
  (if-let [view (domain/queue-view (get-store system store-id) tt)]
    (json-response 200 view)
    (json-response 404 {:error "not-found" :message "unknown table type"})))

(defn- queue-action! [system store-id tt action]
  (let [f (case action
            :open (fn [s] (domain/open-queue s tt))
            :close (fn [s] (domain/close-queue s tt))
            :call (fn [s] (domain/call-next s tt (now-ms)))
            nil)]
    (if (nil? f)
      (json-response 404 {:error "not-found" :message "unknown action"})
      (let [result (transact! system store-id f)]
        (if (:ok result) (json-response 200 (:value result)) (domain-error result))))))

(defn- ticket-action! [system store-id tt id action]
  (let [f (case action
            :seat (fn [s] (domain/seat s tt id (now-ms)))
            :no-show (fn [s] (domain/no-show s tt id (now-ms)))
            :cancel (fn [s] (domain/cancel s tt id))
            nil)]
    (if (nil? f)
      (json-response 404 {:error "not-found" :message "unknown action"})
      (let [result (transact! system store-id f)]
        (if (:ok result) (json-response 200 (:value result)) (domain-error result))))))

(defn- sse! [system req store-id token]
  (let [store (get-store system store-id)]
    (if-not (domain/ticket-by-token store token)
      (json-response 404 {:error "not-found" :message "ticket not found"})
      (http/as-channel req
        {:on-open (fn [ch]
                    (subscribe! (:subs system) :by-token token ch)
                    (http/send! ch
                      {:status 200
                       :headers {"Content-Type" "text/event-stream; charset=utf-8"
                                 "Cache-Control" "no-cache"
                                 "Connection" "keep-alive"}
                       :body (sse-frame :snapshot (domain/ticket-status-view store token))}
                      false))
         :on-close (fn [ch _status]
                     (unsubscribe! (:subs system) :by-token token ch))}))))

(defn- dispatch [system req]
  (let [m (:request-method req)
        s (segments (:uri req))]
    (cond
      (and (= :get m) (empty? s))
      (home system)

      (and (= :post m) (= 3 (count s)) (= "stores" (nth s 0)) (= "tickets" (nth s 2)))
      (create-ticket! system (nth s 1) req)

      (and (= :get m) (= 4 (count s)) (= "stores" (nth s 0)) (= "tickets" (nth s 2)))
      (ticket-status! system (nth s 1) (nth s 3))

      (and (= :post m) (= 5 (count s)) (= "stores" (nth s 0)) (= "tickets" (nth s 2)) (= "cancel" (nth s 4)))
      (cancel-by-token! system (nth s 1) (nth s 3))

      (and (= :get m) (= 4 (count s)) (= "stores" (nth s 0)) (= "queues" (nth s 2)))
      (queue-status! system (nth s 1) (keyword (nth s 3)))

      (and (= :post m) (= 5 (count s)) (= "stores" (nth s 0)) (= "queues" (nth s 2)))
      (queue-action! system (nth s 1) (keyword (nth s 3)) (keyword (nth s 4)))

      (and (= :post m) (= 7 (count s)) (= "stores" (nth s 0)) (= "queues" (nth s 2)) (= "tickets" (nth s 4)))
      (ticket-action! system (nth s 1) (keyword (nth s 3)) (nth s 5) (keyword (nth s 6)))

      (and (= :get m) (= 4 (count s)) (= "stores" (nth s 0)) (= "events" (nth s 2)))
      (sse! system req (nth s 1) (nth s 3))

      :else
      (json-response 404 {:error "not-found" :message "no such route"}))))

(defn handler [system]
  (fn [req]
    (try
      (dispatch system req)
      (catch Exception e
        (json-response 500 {:error "internal" :message (str e)})))))
