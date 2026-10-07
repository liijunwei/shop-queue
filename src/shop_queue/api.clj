(ns shop-queue.api
  "HTTP layer: a hand-written router over http-kit, plus the SSE subscription
  registry. The domain stays pure; IO (JSON, SQLite, channels) lives here."
  (:require [cheshire.core :as json]
            [clojure.string :as str]
            [next.jdbc :as jdbc]
            [org.httpkit.server :as http]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage]))

(def write-lock (Object.))

(def max-event-log
  "Bounded per-token SSE replay history. Missed events older than this window
  are covered by the authoritative snapshot, not by replay."
  100)

(defn- now-epoch-ms [] (System/currentTimeMillis))

(def ^:private rfc3339-formatter
  (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd'T'HH:mm:ssXXX"))

(defn- rfc3339-now []
  (.format (java.time.OffsetDateTime/now) rfc3339-formatter))

(defn- json-response [status body]
  {:status status
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (json/generate-string body)})

(defn- error-status [code]
  (case code
    (:invalid-party-size :party-size-too-large :invalid-source :no-matching-table-type :invalid-threshold) 400
    :not-found 404
    (:queue-closed :queue-drained :empty-queue :invalid-transition :queue-too-long :token-conflict) 409
    500))

(defn- domain-error [result]
  (let [{:keys [code message]} (:error result)]
    (json-response (error-status code) {:error (name code) :message message})))

(defn- not-found [message]
  (json-response 404 {:error "not-found" :message message}))

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

(defn request-log-fields
  "Derive the structured fields carried by a request log line: :store-id and
  :ticket-id when the URI path identifies them, plus :method and :uri."
  [req]
  (let [s (segments (:uri req))]
    {:method (some-> (:request-method req) name)
     :uri (:uri req)
     :store-id (when (and (= "stores" (first s)) (nth s 1 nil)) (nth s 1))
     :ticket-id (when (and (= "stores" (first s))
                           (= "queues" (nth s 2 nil))
                           (= "tickets" (nth s 4 nil))
                           (nth s 5 nil))
                  (nth s 5))}))

(defn- query-params [req]
  (let [s (:query-string req)]
    (if (str/blank? s)
      {}
      (into {}
            (for [pair (str/split s #"&")
                  :let [[k v] (str/split pair #"=" 2)]
                  :when (not (str/blank? k))]
              [(java.net.URLDecoder/decode k "UTF-8")
               (java.net.URLDecoder/decode (or v "") "UTF-8")])))))

(defn- query-param [req k]
  (get (query-params req) k))

(defn- parse-long-str [s]
  (when (and s (not (str/blank? s)))
    (try (Long/parseLong s) (catch NumberFormatException _ nil))))

(defn- parse-int-param [req k default]
  (or (parse-long-str (query-param req k)) default))

;; ---- SSE registry (IO state, never inside domain state) ----

(defn- subscribe! [subs kind key ch]
  (swap! subs update-in [kind key] (fnil conj #{}) ch))

(defn- unsubscribe! [subs kind key ch]
  (swap! subs update-in [kind key] (fnil disj #{}) ch))

(defn- sse-frame [event-type id data]
  (str "event: " (name event-type) "\n"
       "id: " id "\n"
       "data: " (json/generate-string data) "\n\n"))

(defn- send-event! [ch id event-type data]
  (http/send! ch (sse-frame event-type id data) false))

(defn- next-seq!
  "Advance and return the process-wide monotonic SSE sequence number. The
  sequence is global (not per ticket) so ordering is total and gaps are
  allowed; a ticket stream only needs monotonicity."
  [events]
  (:seq (swap! events update :seq inc)))

(defn- remember-event! [events token ev]
  (swap! events update-in [:by-token token]
         (fn [log] (vec (take-last max-event-log (conj (or log []) ev))))))

(defn- replay-frames [events-snapshot token last-id]
  (when last-id
    (->> (get-in events-snapshot [:by-token token])
         (filter #(> (:seq %) last-id))
         (mapv (fn [ev] (sse-frame (:type ev) (:seq ev) ev))))))

(defn- publish-event! [system ev]
  (let [{:keys [events subs]} system]
    (case (:kind ev)
      :ticket
      (let [seq (next-seq! events)
            payload {:seq seq :type (:type ev) :ticket-id (:ticket-id ev) :table-type (:table-type ev)}]
        (remember-event! events (:token ev) payload)
        (doseq [ch (get-in @subs [:by-token (:token ev)])]
          (send-event! ch seq (:type ev) payload)))
      :queue
      (let [seq (next-seq! events)
            payload {:seq seq :type (:type ev) :table-type (:table-type ev)}]
        (doseq [ch (get-in @subs [:by-table-type (:table-type ev)])]
          (send-event! ch seq (:type ev) payload)))
      nil)))

;; ---- State access + write path ----

(defn- get-store [system store-id]
  (when-let [store (get-in @(:state system) [:stores store-id])]
    (domain/seed-store store)))

(defn- op [actor actor-id action target-fn]
  (fn [result]
    (let [target (target-fn result)]
      (when target
        {:ts (now-epoch-ms)
         :actor actor
         :actor-id actor-id
         :action action
         :target target
         :result (if (:ok result) :ok :error)
         :details (if (:ok result) (:value result) (:error result))}))))

(defn- queue-op [action tt]
  (let [action' (case action
                  :open :open-queue
                  :close :close-queue
                  :drain :drain-queue
                  :call :call)]
    (op :staff "staff" action'
        (fn [result]
          (if (and (= :call action') (:ok result))
            {:kind :ticket :id (:id (:value result))}
            {:kind :queue :table-type tt})))))

(defn- transact! [system store-id f op]
  (locking write-lock
    (let [old-store (get-store system store-id)
          result (f old-store)
          record (when op (op result))]
      (when (:ok result)
        (jdbc/with-transaction [tx (:db system)]
          (storage/save-store! tx store-id (:state result))
          (when record
            (storage/append-operation! tx store-id record)))
        (swap! (:state system) assoc-in [:stores store-id] (:state result))
        (doseq [ev (domain/diff-events old-store (:state result))]
          (publish-event! system ev)))
      (when (and record (not (:ok result)))
        (storage/append-operation! (:db system) store-id record))
      result)))

(defn- transact-create-ticket! [system store-id party-size source idempotency-key token id]
  (locking write-lock
    (let [old-store (get-store system store-id)
          plan (domain/ticket-plan old-store party-size source idempotency-key)]
      (case (:kind plan)
        :error
        (:error plan)

        :duplicate
        (domain/create-ticket old-store party-size source (now-epoch-ms) token id idempotency-key)

        :new
        (let [result (jdbc/with-transaction [tx (:db system)]
                       (if-not (storage/claim-token! tx store-id token)
                         {:ok false :error {:code :token-conflict :message "token already exists"}}
                         (let [tt (:table-type plan)
                               current (long (get-in old-store [:queues tt :counter] 0))
                               n (storage/next-counter! tx store-id tt current)
                               result (domain/create-ticket old-store party-size source (now-epoch-ms) token id idempotency-key n)
                               new-store (:state result)
                               record ((op :customer token :create-ticket
                                         (fn [r] (when-not (:duplicate (:value r))
                                                   {:kind :ticket :id (:id (:value r))})))
                                       result)]
                           (storage/save-store! tx store-id new-store)
                           (when record
                             (storage/append-operation! tx store-id record))
                           result)))]
          (when (:ok result)
            (swap! (:state system) assoc-in [:stores store-id] (:state result))
            (doseq [ev (domain/diff-events old-store (:state result))]
              (publish-event! system ev)))
          result)))))

;; ---- Handlers ----

(defn- home [system]
  (json-response 200
    {:stores (into {}
                   (map (fn [[id s]] [id (domain/store-view s)]))
                   (:stores @(:state system))) }))

(defn- healthz! [system]
  (if (storage/writable? (:db system))
    (json-response 200 {:status "ok" :db "writable"})
    (json-response 503 {:status "unavailable" :db "not-writable"})))

(defn- create-ticket! [system store-id req]
  (if-not (get-store system store-id)
    (not-found "store not found")
    (let [body (parse-json req)
          party-size (:party-size body)
          source (or (:source body) "remote")
          idempotency-key (not-empty (:idempotency-key body))]
      (if (not (domain/valid-party-size? party-size))
        (json-response 400 {:error "invalid-party-size" :message "party-size must be a positive integer"})
        (let [token (str (java.util.UUID/randomUUID))
              id (str (java.util.UUID/randomUUID))
              result (transact-create-ticket! system store-id party-size source idempotency-key token id)]
          (if (:ok result)
            (json-response (if (:duplicate (:value result)) 200 201) (:value result))
            (domain-error result)))))))

(defn- ticket-status! [system store-id token]
  (if-let [view (some-> (get-store system store-id)
                        (domain/ticket-status-view token))]
    (json-response 200 view)
    (not-found "ticket not found")))

(defn- cancel-by-token! [system store-id token]
  (if-let [t (some-> (get-store system store-id)
                     (domain/ticket-by-token token))]
    (let [id (:id t)
          result (transact! system store-id
                   (fn [s] (domain/cancel s (:table-type t) id))
                   (op :customer token :cancel (constantly {:kind :ticket :id id})))]
      (if (:ok result) (json-response 200 (:value result)) (domain-error result)))
    (not-found "ticket not found")))

(defn- queue-status! [system store-id tt]
  (if-let [view (some-> (get-store system store-id)
                        (domain/queue-view tt))]
    (json-response 200 view)
    (not-found "unknown table type")))

(defn- queue-action! [system store-id tt action]
  (let [f (case action
            :open (fn [s] (domain/open-queue s tt))
            :close (fn [s] (domain/close-queue s tt))
            :drain (fn [s] (domain/drain-queue s tt))
            :call (fn [s] (domain/call-next s tt (now-epoch-ms)))
            nil)]
    (cond
      (nil? f)
      (not-found "unknown action")

      (nil? (get-store system store-id))
      (not-found "store not found")

      :else
      (let [result (transact! system store-id f (queue-op action tt))]
        (if (:ok result) (json-response 200 (:value result)) (domain-error result))))))

(defn- set-threshold! [system store-id req]
  (if-not (get-store system store-id)
    (not-found "store not found")
    (let [body (parse-json req)
          threshold (:threshold body)]
      (if (not (and (integer? threshold) (not (neg? threshold))))
        (json-response 400 {:error "invalid-threshold"
                            :message "requeue-threshold must be a non-negative integer"})
        (let [result (transact! system store-id
                       (fn [s] (domain/set-requeue-threshold s threshold))
                       (op :staff "staff" :set-requeue-threshold
                           (constantly {:kind :store :requeue-threshold threshold})))]
          (if (:ok result) (json-response 200 (:value result)) (domain-error result)))))))

(defn- ticket-action! [system store-id tt id action]
  (let [f (case action
            :seat (fn [s] (domain/seat s tt id (now-epoch-ms)))
            :no-show (fn [s] (domain/no-show s tt id (now-epoch-ms)))
            :requeue (fn [s] (domain/requeue s tt id (now-epoch-ms)))
            :cancel (fn [s] (domain/cancel s tt id))
            nil)]
    (cond
      (nil? f)
      (not-found "unknown action")

      (nil? (get-store system store-id))
      (not-found "store not found")

      :else
      (let [result (transact! system store-id f
                      (op :staff "staff" action (constantly {:kind :ticket :id id})))]
        (if (:ok result) (json-response 200 (:value result)) (domain-error result))))))

(defn- operations-list! [system store-id req]
  (if-not (get-store system store-id)
    (not-found "store not found")
    (let [ops (storage/load-operations (:db system) store-id)
          actor (some-> (query-param req "actor") not-empty keyword)
          action (some-> (query-param req "action") not-empty keyword)
          since (parse-long-str (query-param req "since"))
          limit (max 1 (min 1000 (parse-int-param req "limit" 100)))
          offset (max 0 (parse-int-param req "offset" 0))
          filtered (domain/query-operations ops {:actor actor :action action :since since})
          total (count filtered)
          page (->> filtered (drop offset) (take limit) vec)]
      (json-response 200 {:operations page :total total :limit limit :offset offset}))))

(defn- ticket-operations! [system store-id token]
  (if-let [t (some-> (get-store system store-id)
                     (domain/ticket-by-token token))]
    (let [id (:id t)
          timeline (->> (storage/load-operations (:db system) store-id)
                        (filter #(and (= :ticket (:kind (:target %)))
                                      (= id (:id (:target %)))))
                        vec)]
      (json-response 200 {:ticket-id id :operations timeline}))
    (not-found "ticket not found")))

(defn- sse! [system req store-id token]
  (if-let [store (get-store system store-id)]
    (if-not (domain/ticket-by-token store token)
      (not-found "ticket not found")
      (http/as-channel req
        {:on-open (fn [ch]
                    (subscribe! (:subs system) :by-token token ch)
                    (let [store (get-store system store-id)
                          events-snapshot @(:events system)
                          last-id (parse-long-str (get-in req [:headers "last-event-id"]))
                          seq (:seq events-snapshot)
                          replay (replay-frames events-snapshot token last-id)
                          snapshot-data (assoc (domain/ticket-status-view store token)
                                               :seq seq
                                               :last-event-id last-id)
                          body (str (apply str replay)
                                    (sse-frame :snapshot seq snapshot-data))]
                      (http/send! ch
                        {:status 200
                         :headers {"Content-Type" "text/event-stream; charset=utf-8"
                                   "Cache-Control" "no-cache"
                                   "Connection" "keep-alive"}
                         :body body}
                        false)))
         :on-close (fn [ch _status]
                     (unsubscribe! (:subs system) :by-token token ch))}))
    (not-found "ticket not found")))

(defn- dispatch [system req]
  (let [m (:request-method req)
        s (segments (:uri req))]
    (cond
      (and (= :get m) (= 1 (count s)) (= "healthz" (nth s 0)))
      (healthz! system)

      (and (= :get m) (empty? s))
      (home system)

      (and (= :post m) (= 3 (count s)) (= "stores" (nth s 0)) (= "tickets" (nth s 2)))
      (create-ticket! system (nth s 1) req)

      (and (= :post m) (= 3 (count s)) (= "stores" (nth s 0)) (= "requeue-threshold" (nth s 2)))
      (set-threshold! system (nth s 1) req)

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

      (and (= :get m) (= 3 (count s)) (= "stores" (nth s 0)) (= "operations" (nth s 2)))
      (operations-list! system (nth s 1) req)

      (and (= :get m) (= 5 (count s)) (= "stores" (nth s 0)) (= "tickets" (nth s 2)) (= "operations" (nth s 4)))
      (ticket-operations! system (nth s 1) (nth s 3))

      :else
      (json-response 404 {:error "not-found" :message "no such route"}))))

(defn- log-request! [fields]
  (println (json/generate-string
            (into {} (remove (comp nil? val)) (assoc fields :time (rfc3339-now))))))

(defn handler [system]
  (fn [req]
    (let [request-id (str (java.util.UUID/randomUUID))
          start (System/nanoTime)
          base (request-log-fields req)
          duration-ms (fn [] (long (/ (- (System/nanoTime) start) 1000000)))]
      (try
        (let [resp (dispatch system req)]
          (log-request! (assoc base
                               :event "request"
                               :request-id request-id
                               :status (:status resp)
                               :duration-ms (duration-ms)))
          resp)
        (catch Exception e
          (log-request! (assoc base
                               :event "request"
                               :request-id request-id
                               :status 500
                               :duration-ms (duration-ms)))
          (json-response 500 {:error "internal" :message (str e)}))))))
