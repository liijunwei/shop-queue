(ns shop-queue.domain
  "Pure domain functions for the shop queue. State is ordinary immutable data;
  every command returns either {:ok true :state new-store :value ...} or
  {:ok false :error {:code .. :message ..}}. No IO happens here.")

(def default-table-types
  {:two   {:label "2人"  :min 2 :max 2}
   :three {:label "3人"  :min 3 :max 3}
   :large {:label "多人" :min 4 :max nil}})

(def max-party-size 50)
(def valid-sources #{"remote" "scan"})

(defn- ok [state value] {:ok true :state state :value value})
(defn- err [code message] {:ok false :error {:code code :message message}})

(defn valid-party-size? [n]
  (and (integer? n) (pos? n)))

;; Ensure a store carries every configured table-type queue plus a :tickets map.
(defn seed-store
  ([] (seed-store {}))
  ([store]
   (let [table-types (merge default-table-types (:table-types store))
         queues (into {}
                      (map (fn [[tt _]]
                             [tt (merge {:status :open :counter 0}
                                        (get-in store [:queues tt]))])
                           table-types))]
     (-> store
         (assoc :table-types table-types)
         (assoc :queues queues)
         (update :tickets #(or % {}))))))

;; Smallest capacity that can seat n; :max nil means no upper bound.
(defn match-table-type [table-types n]
  (->> table-types
       (filter (fn [[_ {:keys [max]}]]
                 (or (nil? max) (>= max n))))
       (sort-by (fn [[_ {:keys [max]}]] (if (nil? max) Long/MAX_VALUE max)))
       ffirst))

;; ---- Derived views (single source of truth is :tickets + :n) ----

(defn tickets-with-id [store]
  (for [[id t] (:tickets store)] (assoc t :id id)))

(defn waiting-tickets [store tt]
  (->> (tickets-with-id store)
       (filter #(and (= tt (:table-type %)) (= :waiting (:status %))))
       (sort-by :n)))

(defn current-called [store tt]
  (->> (tickets-with-id store)
       (filter #(and (= tt (:table-type %)) (= :called (:status %))))
       first))

(defn parties-ahead [store tt n]
  (count (filter #(and (= tt (:table-type %))
                       (= :waiting (:status %))
                       (< (:n %) n))
                 (vals (:tickets store)))))

(defn ticket-by-token [store token]
  (->> (tickets-with-id store)
       (filter #(= token (:token %)))
       first))

;; ---- Commands ----

(defn create-ticket [store party-size source now token id]
  (let [store (seed-store store)
        tt (match-table-type (:table-types store) party-size)]
    (cond
      (not (valid-party-size? party-size))
      (err :invalid-party-size "party-size must be a positive integer")

      (> party-size max-party-size)
      (err :party-size-too-large (str "party-size exceeds " max-party-size))

      (not (contains? valid-sources source))
      (err :invalid-source "source must be remote or scan")

      (nil? tt)
      (err :no-matching-table-type "party-size exceeds every table size")

      (= :closed (get-in store [:queues tt :status]))
      (err :queue-closed "queue is closed")

      :else
      (let [n (inc (long (get-in store [:queues tt :counter] 0)))
            ticket {:n n :token token :table-type tt :party-size party-size
                    :source source :status :waiting :created-at now :called-at nil}
            new-store (-> store
                          (assoc-in [:queues tt :counter] n)
                          (assoc-in [:tickets id] ticket))]
        (ok new-store {:id id :token token :table-type tt :party-size party-size
                       :parties-ahead (parties-ahead new-store tt n)})))))

(defn call-next [store tt now]
  (let [store (seed-store store)]
    (cond
      (not (contains? (:table-types store) tt))
      (err :not-found "unknown table type")

      (current-called store tt)
      (err :ticket-in-flight (str "call " (:id (current-called store tt)) " first"))

      (empty? (waiting-tickets store tt))
      (err :empty-queue "no waiting tickets")

      :else
      (let [nxt (first (waiting-tickets store tt))
            new-store (-> store
                          (assoc-in [:tickets (:id nxt) :status] :called)
                          (assoc-in [:tickets (:id nxt) :called-at] now))]
        (ok new-store {:id (:id nxt) :token (:token nxt)})))))

(defn- finalize [store tt id to-status]
  (let [store (seed-store store)
        t (get-in store [:tickets id])]
    (cond
      (nil? t) (err :not-found "ticket not found")
      (not= tt (:table-type t)) (err :not-found "ticket not found")
      (not= :called (:status t)) (err :invalid-transition "ticket is not called")
      :else (ok (assoc-in store [:tickets id :status] to-status) {:id id}))))

(defn seat [store tt id _now] (finalize store tt id :seated))
(defn no-show [store tt id _now] (finalize store tt id :no-show))

(defn cancel [store tt id]
  (let [store (seed-store store)
        t (get-in store [:tickets id])]
    (cond
      (nil? t) (err :not-found "ticket not found")
      (not= tt (:table-type t)) (err :not-found "ticket not found")
      (not= :waiting (:status t)) (err :invalid-transition "ticket is not waiting")
      :else (ok (assoc-in store [:tickets id :status] :cancelled) {:id id}))))

(defn set-queue-status [store tt status]
  (let [store (seed-store store)]
    (if (not (contains? (:table-types store) tt))
      (err :not-found "unknown table type")
      (ok (assoc-in store [:queues tt :status] status)
          {:table-type tt :status status}))))

(defn open-queue [store tt] (set-queue-status store tt :open))
(defn close-queue [store tt] (set-queue-status store tt :closed))

;; ---- Events as a pure projection of the value change ----

(defn diff-events [old-store new-store]
  (let [old-tickets (:tickets old-store)
        new-tickets (:tickets new-store)
        ids (into (set (keys old-tickets)) (keys new-tickets))
        ticket-events
        (for [id ids
              :let [o (:status (get old-tickets id))
                    n (:status (get new-tickets id))]
              :when (and o n (not= o n))]
          {:kind :ticket :type n :ticket-id id
           :token (:token (get new-tickets id))
           :table-type (:table-type (get new-tickets id))})
        queue-events
        (for [[tt new-q] (:queues new-store)
              :let [old-q (get-in old-store [:queues tt])]
              :when (and old-q (not= (:status old-q) (:status new-q)))]
          {:kind :queue
           :type (if (= :open (:status new-q)) :queue-open :queue-closed)
           :table-type tt})]
    (concat ticket-events queue-events)))

;; ---- Read views for the API ----

(defn ticket-status-view [store token]
  (when-let [t (ticket-by-token store token)]
    (let [tt (:table-type t)]
      {:id (:id t)
       :status (:status t)
       :table-type tt
       :party-size (:party-size t)
       :source (:source t)
       :parties-ahead (parties-ahead store tt (:n t))
       :current-called (some-> (current-called store tt) :id)
       :queue (get-in store [:queues tt :status])})))

(defn queue-view [store tt]
  (let [store (seed-store store)]
    (when (contains? (:table-types store) tt)
      {:table-type tt
       :status (get-in store [:queues tt :status])
       :counter (get-in store [:queues tt :counter])
       :waiting (mapv :id (waiting-tickets store tt))
       :current-called (some-> (current-called store tt) :id)})))

(defn store-view [store]
  (let [store (seed-store store)]
    {:table-types (:table-types store)
     :queues (into {}
                   (map (fn [[tt q]]
                          [tt {:status (:status q)
                               :counter (:counter q)
                               :waiting-count (count (waiting-tickets store tt))
                               :current-called (some-> (current-called store tt) :id)}])
                        (:queues store)))}))
