(ns shop-queue.api
  (:require [clojure.string :as str]
            [cheshire.core :as json]
            [org.httpkit.server :as http]
            [shop-queue.domain :as domain]))

(defonce state (atom (domain/default-state)))
;; token -> #{channel}; SSE subscribers live outside the pure domain state.
(defonce subs (atom {}))

(defn read-json [req]
  (when-let [body (:body req)]
    (json/parse-stream (clojure.java.io/reader body) true)))

(defn json-resp [status data]
  {:status status
   :headers {"Content-Type" "application/json; charset=utf-8"}
   :body (json/generate-string data)})

(defn error-resp [e]
  (let [{:keys [status code] :or {status 500 code :internal}} (ex-data e)]
    (json-resp status {:error code :message (ex-message e)})))

;; Run a mutating domain fn inside swap! and return its full result map.
(defn apply-domain! [f & args]
  (let [res (volatile! nil)]
    (swap! state (fn [s]
                   (let [r (apply f s args)]
                     (vreset! res r)
                     (:state r))))
    @res))

(defn sse-msg [event data]
  (str "event: " event "\ndata: " (json/generate-string data) "\n\n"))

(defn subscribe! [token ch]
  (swap! subs update token (fnil conj #{}) ch))

(defn unsubscribe! [token ch]
  (swap! subs update token (fnil disj #{}) ch)
  (when (empty? (get @subs token))
    (swap! subs dissoc token)))

(defn notify-token! [token event data]
  (doseq [ch (get @subs token)]
    (try (http/send! ch (sse-msg event data) false)
         (catch Exception _))))

(defn notify-table! [store-id tt-k event]
  (let [store (get-in @state [:stores store-id])
        tokens (->> (get-in store [:queues tt-k :order])
                    (map #(get-in store [:tickets % :token]))
                    (filter some?))]
    (doseq [t tokens]
      (notify-token! t event {:table-type (name tt-k)}))))

(defn take-handler [store-id req]
  (let [body (read-json req)
        party-size (:party-size body)
        source (or (:source body) "remote")]
    (when-not (and (integer? party-size) (pos? party-size))
      (throw (domain/err 400 :invalid-party-size "party-size must be a positive integer")))
    (let [{:keys [ticket]} (apply-domain! domain/take-ticket store-id party-size source)]
      (json-resp 201 {:number (:id ticket)
                      :token (:token ticket)
                      :table-type (name (:table-type ticket))
                      :label (get-in domain/table-types [(:table-type ticket) :label])
                      :party-size (:party-size ticket)}))))

(defn status-handler [store-id token]
  (json-resp 200 (domain/status-by-token @state store-id token)))

(defn list-handler [store-id tt-k]
  (json-resp 200 (domain/list-queue @state store-id (keyword tt-k))))

(defn queue-action-handler [store-id tt-k action]
  (let [tt (keyword tt-k)]
    (case action
      "open" (do (apply-domain! domain/set-queue-status store-id tt :open)
                 (notify-table! store-id tt "queue-open")
                 (json-resp 200 {:table-type (name tt) :status :open}))
      "close" (do (apply-domain! domain/set-queue-status store-id tt :closed)
                  (notify-table! store-id tt "queue-closed")
                  (json-resp 200 {:table-type (name tt) :status :closed}))
      "call" (let [{:keys [ticket]} (apply-domain! domain/call-next store-id tt)]
               (when ticket (notify-token! (:token ticket) "called" {:number (:id ticket)}))
               (if ticket
                 (json-resp 200 {:called (:id ticket)})
                 (json-resp 200 {:called nil :message "queue empty"})))
      (json-resp 400 {:error :bad-action :message (str "unknown action " action)}))))

(defn ticket-action-handler [store-id tt-k ticket-id action]
  (case action
    "seat" (let [{:keys [ticket]} (apply-domain! domain/seat store-id ticket-id)]
             (notify-token! (:token ticket) "seated" {:number (:id ticket)})
             (json-resp 200 {:number (:id ticket) :status :seated}))
    "no-show" (let [{:keys [ticket]} (apply-domain! domain/no-show store-id ticket-id)]
                (notify-token! (:token ticket) "no-show" {:number (:id ticket)})
                (json-resp 200 {:number (:id ticket) :status :no-show}))
    "cancel" (let [{:keys [ticket]} (apply-domain! domain/cancel store-id ticket-id)]
               (notify-token! (:token ticket) "cancelled" {:number (:id ticket)})
               (json-resp 200 {:number (:id ticket) :status :cancelled}))
    (json-resp 400 {:error :bad-action :message (str "unknown action " action)})))

(defn events-handler [store-id token req]
  (http/with-channel req ch
    (http/on-close ch (fn [_] (unsubscribe! token ch)))
    (subscribe! token ch)
    (try
      (http/send! ch (sse-msg "snapshot" (domain/status-by-token @state store-id token)) false)
      (catch clojure.lang.ExceptionInfo e
        (http/send! ch (sse-msg "error" {:message (ex-message e)}) true)))))

(defn route [req]
  (let [m (:request-method req)
        segs (vec (remove str/blank? (str/split (:uri req) #"/")))]
    (cond
      (and (= m :get) (empty? segs))
      (json-resp 200 {:status "ok" :store "store-1" :table-types (mapv (fn [[k v]] {:id (name k) :label (:label v)}) domain/table-types)})

      (and (= m :post) (= 3 (count segs)) (= "stores" (nth segs 0)) (= "tickets" (nth segs 2)))
      (take-handler (nth segs 1) req)

      (and (= m :get) (= 4 (count segs)) (= "stores" (nth segs 0)) (= "tickets" (nth segs 2)))
      (status-handler (nth segs 1) (nth segs 3))

      (and (= m :get) (= 4 (count segs)) (= "stores" (nth segs 0)) (= "queues" (nth segs 2)))
      (list-handler (nth segs 1) (nth segs 3))

      (and (= m :post) (= 5 (count segs)) (= "stores" (nth segs 0)) (= "queues" (nth segs 2)))
      (queue-action-handler (nth segs 1) (nth segs 3) (nth segs 4))

      (and (= m :post) (= 7 (count segs)) (= "stores" (nth segs 0)) (= "queues" (nth segs 2)) (= "tickets" (nth segs 4)))
      (ticket-action-handler (nth segs 1) (nth segs 3) (nth segs 5) (nth segs 6))

      (and (= m :get) (= 4 (count segs)) (= "stores" (nth segs 0)) (= "events" (nth segs 2)))
      (events-handler (nth segs 1) (nth segs 3) req)

      :else (json-resp 404 {:error :not-found :message "no such route"}))))

(defn handler [req]
  (try
    (route req)
    (catch clojure.lang.ExceptionInfo e (error-resp e))
    (catch Exception e (json-resp 500 {:error :internal :message (str e)}))))

(defn start! [port]
  (http/run-server handler {:port port}))
