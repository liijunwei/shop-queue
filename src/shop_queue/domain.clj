(ns shop-queue.domain
  "Pure domain functions for the shop queue. State is ordinary immutable data;
  every command returns either {:ok true :state new-store :value ...} or
  {:ok false :error {:code .. :message ..}}. No IO happens here."
  (:require [clojure.string :as str]))

(def default-table-types
  {:two   {:label "2人"  :min 2 :max 2}
   :three {:label "3人"  :min 3 :max 3}
   :large {:label "多人" :min 4 :max nil}})

(def max-party-size 50)
(def valid-sources #{"remote" "scan"})
(def default-requeue-threshold 10)

(defn- ok [state value] {:ok true :state state :value value})
(defn- err [code message] {:ok false :error {:code code :message message}})

(defn valid-party-size? [n]
  (and (integer? n) (pos? n)))

(defn- live-queue-types
  "Every table type that still has a live queue: the configured types plus any
  type still referenced by an issued ticket. Removing a type from configuration
  must not orphan its already-issued tickets (config-snapshot semantics)."
  [store]
  (into (set (keys (:table-types store)))
        (map :table-type (vals (or (:tickets store) {})))))

;; Ensure a store carries a queue for every live table type plus a :tickets map.
;; `:table-types` is authoritative once present: configuration changes must not
;; be silently undone by re-merging the defaults.
(defn seed-store
  ([] (seed-store {}))
  ([store]
   (let [table-types (if (map? (:table-types store))
                       (:table-types store)
                       default-table-types)
         queues (into {}
                      (map (fn [tt]
                             [tt (merge {:status :open :counter 0}
                                        (get-in store [:queues tt]))])
                           (live-queue-types (assoc store :table-types table-types))))]
     (-> store
         (assoc :table-types table-types)
         (assoc :queues queues)
         (assoc :name (:name store))
         (assoc :status (or (:status store) :active))
         (update :tickets #(or % {}))
         (update :idempotency #(or % {}))
         (update :requeue-threshold #(or % default-requeue-threshold))
         (update :version #(or % 0))))))

;; Match a party of n to the smallest table type that can seat it:
;; - candidate table types must satisfy capacity (:max >= n; :max nil means no
;;   upper bound);
;; - among candidates, pick the one with the smallest :min (ascending).
(defn match-table-type [table-types n]
  (->> table-types
       (filter (fn [[_ {:keys [max]}]]
                 (or (nil? max) (>= max n))))
       (sort-by (fn [[_ {:keys [min]}]] min))
       ffirst))

(defn set-table-types
  "Replace the table-type configuration. This is a pure configuration change:
  it only affects which type future create-ticket requests match to. Tickets
  already issued keep their recorded :table-type and continue to be called /
  seated normally (config-snapshot semantics)."
  [store table-types]
  (if (map? table-types)
    (let [store (seed-store store)]
      (ok (seed-store (assoc store :table-types table-types))
          {:table-types table-types}))
    (err :invalid-table-types "table-types must be a map")))

(defn valid-store-name? [name]
  (and (string? name) (not (str/blank? name))))

(defn create-store
  "Assemble a fresh store from optional admin config: `:name` (display name,
  separate from the store id) and `:table-types`. A store starts active, with
  its three default table-type queues open."
  ([] (create-store {}))
  ([{:keys [name table-types]}]
   (cond
     (and (some? name) (not (valid-store-name? name)))
     (err :invalid-store-name "store name must be a non-blank string")

     (and (some? table-types) (not (map? table-types)))
     (err :invalid-table-types "table-types must be a map")

     :else
     (let [store (seed-store (cond-> {}
                               (some? name) (assoc :name (str/trim name))
                               (some? table-types) (assoc :table-types table-types)))]
       (ok store {:name (:name store) :table-types (:table-types store)})))))

(defn archive-store
  "Soft-delete a store: mark it archived without discarding its persisted
  document or operation history."
  [store]
  (ok (assoc (seed-store store) :status :archived) {:status :archived}))

(defn apply-store-config
  "Apply an optional partial admin update to `:name` and `:requeue-threshold`.
  `:table-types` changes stay on `set-table-types` so config-snapshot semantics
  are preserved."
  [store {:keys [name requeue-threshold]}]
  (let [store (seed-store store)]
    (cond
      (and (some? name) (not (valid-store-name? name)))
      (err :invalid-store-name "store name must be a non-blank string")

      (and (some? requeue-threshold)
           (not (and (integer? requeue-threshold) (not (neg? requeue-threshold)))))
      (err :invalid-threshold "requeue-threshold must be a non-negative integer")

      :else
      (let [updated (cond-> store
                      (some? name) (assoc :name (str/trim name))
                      (some? requeue-threshold) (assoc :requeue-threshold requeue-threshold))]
        (ok updated {:name (:name updated)
                     :requeue-threshold (:requeue-threshold updated)})))))

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

(defn- ticket-create-value [store t]
  (let [tt (:table-type t)]
    {:id (:id t)
     :token (:token t)
     :table-type tt
     :party-size (:party-size t)
     :parties-ahead (parties-ahead store tt (:n t))}))

(defn ticket-plan
  "Decide what a create-ticket request should do without consuming a counter
  number. Returns {:kind :error :error m}, {:kind :duplicate :ticket t}, or
  {:kind :new :table-type tt}. This lets the API layer allocate the next number
  from storage only when a new ticket will actually be created."
  [store party-size source idempotency-key]
  (let [store (seed-store store)
        existing (when idempotency-key
                   (when-let [existing-id (get-in store [:idempotency idempotency-key])]
                     (when-let [t (get-in store [:tickets existing-id])]
                       (assoc t :id existing-id))))]
    (cond
      (not (valid-party-size? party-size))
      {:kind :error :error (err :invalid-party-size "party-size must be a positive integer")}

      (> party-size max-party-size)
      {:kind :error :error (err :party-size-too-large (str "party-size exceeds " max-party-size))}

      (not (contains? valid-sources source))
      {:kind :error :error (err :invalid-source "source must be remote or scan")}

      existing
      {:kind :duplicate :ticket existing}

      :else
      (let [tt (match-table-type (:table-types store) party-size)]
        (cond
          (nil? tt)
          {:kind :error :error (err :no-matching-table-type "party-size exceeds every table size")}

          (not= :open (get-in store [:queues tt :status]))
          {:kind :error :error (err :queue-closed "queue is closed")}

          :else
          {:kind :new :table-type tt})))))

(defn- build-new-ticket [store n party-size source now token id idempotency-key tt]
  (let [ticket {:n n :token token :table-type tt :party-size party-size
                :source source :status :waiting :created-at now :called-at nil}
        new-store (-> store
                      (assoc-in [:queues tt :counter] n)
                      (assoc-in [:tickets id] ticket)
                      (cond-> idempotency-key
                        (assoc-in [:idempotency idempotency-key] id)))]
    (ok new-store (ticket-create-value new-store (assoc ticket :id id)))))

(defn create-ticket
  "Create a ticket. When a non-nil `idempotency-key` has already been used in
  this store, return the existing ticket unchanged and flag the replay with
  :duplicate true; state is left untouched.

  The 8-arity form takes the next sequential number `n` already allocated by
  the storage layer, keeping the counter allocation atomic and outside the pure
  domain; the shorter arities derive `n` from the store counter for pure use."
  ([store party-size source now token id]
   (create-ticket store party-size source now token id nil))
  ([store party-size source now token id idempotency-key]
   (let [store (seed-store store)
         plan (ticket-plan store party-size source idempotency-key)]
     (case (:kind plan)
       :error (:error plan)
       :duplicate (ok store (assoc (ticket-create-value store (:ticket plan)) :duplicate true))
       :new (let [tt (:table-type plan)
                  n (inc (long (get-in store [:queues tt :counter] 0)))]
              (build-new-ticket store n party-size source now token id idempotency-key tt)))))
  ([store party-size source now token id idempotency-key n]
   (let [store (seed-store store)
         plan (ticket-plan store party-size source idempotency-key)]
     (case (:kind plan)
       :error (:error plan)
       :duplicate (ok store (assoc (ticket-create-value store (:ticket plan)) :duplicate true))
       :new (build-new-ticket store n party-size source now token id idempotency-key
                              (:table-type plan))))))

(defn call-next [store tt now]
  (let [store (seed-store store)]
    (cond
      (not (contains? (:queues store) tt))
      (err :not-found "unknown table type")

      (= :drained (get-in store [:queues tt :status]))
      (err :queue-drained "queue is drained")

      (empty? (waiting-tickets store tt))
      (err :empty-queue "no waiting tickets")

      :else
      (let [nxt (first (waiting-tickets store tt))
            prev (current-called store tt)
            ;; At most one :called ticket per queue: if the previous called
            ;; ticket is still unhandled, auto-mark it :no-show before advancing.
            no-showed (if prev
                        (assoc-in store [:tickets (:id prev) :status] :no-show)
                        store)
            new-store (-> no-showed
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

(defn requeue
  "过号回来重排：把一张 :no-show 票重新放回该桌型等待队列。
  - 若当前等待队列长度超过 :requeue-threshold（可配置），返回 :queue-too-long，
    提示顾客重新取号（调用 create-ticket）；
  - 否则把票插到队首后两桌的位置（队列不足两桌时排到队尾），实现“向后延两桌”。
  等待队列只按 :n 的相对顺序取号，因此这里对涉及的等待票整体重编号。

  语义约定：:n 是可变队列位次，只对 waiting 票有意义；:counter 是每桌型单调递增的
  发号器，重排不回退它。已终态票保留历史 :n，不参与任何排序/计数。"
  [store tt id _now]
  (let [store (seed-store store)
        t (some-> (get-in store [:tickets id]) (assoc :id id))]
    (cond
      (nil? t) (err :not-found "ticket not found")
      (not= tt (:table-type t)) (err :not-found "ticket not found")
      (not= :no-show (:status t)) (err :invalid-transition "ticket is not no-show")
      (= :drained (get-in store [:queues tt :status])) (err :queue-drained "queue is drained")

      :else
      (let [waiting (waiting-tickets store tt)
            threshold (get store :requeue-threshold default-requeue-threshold)]
        (if (> (count waiting) threshold)
          (err :queue-too-long "queue is too long, please take a new number")
          (let [ahead (take 2 waiting)
                ordered (concat ahead [t] (drop 2 waiting))
                renumbered (map-indexed (fn [i x] (assoc x :n (inc i))) ordered)
                by-id (into {} (map (juxt :id identity)) renumbered)
                new-store (-> store
                              (assoc :tickets (merge (:tickets store) by-id))
                              (assoc-in [:tickets id :status] :waiting)
                              (assoc-in [:tickets id :called-at] nil))]
            (ok new-store {:id id :parties-ahead (count ahead)})))))))

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
    (if (not (contains? (:queues store) tt))
      (err :not-found "unknown table type")
      (ok (assoc-in store [:queues tt :status] status)
          {:table-type tt :status status}))))

(defn open-queue [store tt] (set-queue-status store tt :open))
(defn close-queue [store tt] (set-queue-status store tt :closed))

(defn drain-queue
  "打烊清场：停止叫号（状态置 :drained），并把剩余 :waiting 票逐个标 :cancelled。
  :value 返回被取消的票 id 列表。"
  [store tt]
  (let [store (seed-store store)]
    (cond
      (not (contains? (:queues store) tt))
      (err :not-found "unknown table type")

      :else
      (let [cancelled (mapv :id (waiting-tickets store tt))
            cleared (reduce (fn [s id] (assoc-in s [:tickets id :status] :cancelled))
                            store cancelled)
            new-store (assoc-in cleared [:queues tt :status] :drained)]
        (ok new-store {:table-type tt :status :drained :cancelled cancelled})))))

(defn set-requeue-threshold
  "设置过号重排的队列长度阈值（可配置）。阈值只影响 requeue，不影响取号本身。"
  [store threshold]
  (let [store (seed-store store)]
    (if (and (integer? threshold) (not (neg? threshold)))
      (ok (assoc store :requeue-threshold threshold)
          {:requeue-threshold threshold})
      (err :invalid-threshold "requeue-threshold must be a non-negative integer"))))

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
           :type (cond
                   (= :open (:status new-q)) :queue-open
                   (= :drained (:status new-q)) :queue-drained
                   :else :queue-closed)
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
    (when (contains? (:queues store) tt)
      {:table-type tt
       :status (get-in store [:queues tt :status])
       :counter (get-in store [:queues tt :counter])
       :waiting (mapv :id (waiting-tickets store tt))
       :current-called (some-> (current-called store tt) :id)})))

(defn store-view [store]
  (let [store (seed-store store)]
    {:name (:name store)
     :status (:status store)
     :table-types (:table-types store)
     :requeue-threshold (:requeue-threshold store)
     :queues (into {}
                   (map (fn [[tt q]]
                          [tt {:status (:status q)
                               :counter (:counter q)
                               :waiting-count (count (waiting-tickets store tt))
                               :current-called (some-> (current-called store tt) :id)}])
                        (:queues store)))}))

;; ---- Operation (audit) records ----

(defn query-operations
  "Filter already-sorted operation records. `actor` and `action` are keywords,
  `since` is an epoch-milliseconds timestamp; a nil filter matches everything."
  [operations {:keys [actor action since]}]
  (cond->> operations
    actor (filter #(= actor (:actor %)))
    action (filter #(= action (:action %)))
    since (filter #(>= (long (:ts %)) since))))
