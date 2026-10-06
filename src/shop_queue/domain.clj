(ns shop-queue.domain)

;; State is plain data (DOP): a map of stores -> table types / queues / tickets.
;; All functions here are pure: they take a state map and return a new one.

(def table-types
  {:two   {:label "2人" :prefix "A" :min 2 :max 2}
   :three {:label "3人" :prefix "B" :min 3 :max 3}
   :large {:label "多人" :prefix "C" :min 4 :max nil}})

(defn err [status code msg]
  (ex-info msg {:status status :code code}))

(defn default-state []
  {:stores
   {"store-1"
    {:table-types table-types
     :queues {:two   {:status :open :counter 0 :order []}
              :three {:status :open :counter 0 :order []}
              :large {:status :open :counter 0 :order []}}
     :tickets {}
     :by-token {}}}})

(defn pick-table-type [size]
  (->> table-types
       (filter (fn [[_ tt]] (or (nil? (:max tt)) (<= size (:max tt)))))
       (sort-by (fn [[_ tt]] (:min tt)))
       first))

(defn take-ticket [state store-id party-size source]
  (let [store (get-in state [:stores store-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (let [[tt-k tt-v] (pick-table-type party-size)]
      (when-not tt-k (throw (err 400 :unsupported-party-size "no table type fits this party size")))
      (let [q (get-in store [:queues tt-k])]
        (when-not (= :open (:status q)) (throw (err 409 :queue-closed "queue is closed")))
        (let [counter (inc (:counter q))
              number (str (:prefix tt-v) (format "%03d" counter))
              token (str (java.util.UUID/randomUUID))
              ticket {:id number
                      :token token
                      :table-type tt-k
                      :party-size party-size
                      :source source
                      :status :waiting
                      :created-at (System/currentTimeMillis)
                      :called-at nil}
              new-state (-> state
                            (assoc-in [:stores store-id :queues tt-k :counter] counter)
                            (update-in [:stores store-id :queues tt-k :order] conj number)
                            (assoc-in [:stores store-id :tickets number] ticket)
                            (assoc-in [:stores store-id :by-token token] number))]
          {:state new-state :ticket ticket})))))

(defn call-next [state store-id tt-k]
  (let [store (get-in state [:stores store-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (let [q (get-in store [:queues tt-k])]
      (when-not q (throw (err 404 :table-type-not-found "table type not found")))
      (let [ticket-id (->> (:order q)
                           (filter #(= :waiting (get-in store [:tickets % :status])))
                           first)]
        (if-not ticket-id
          {:state state :ticket nil}
          (let [now (System/currentTimeMillis)
                new-state (-> state
                              (assoc-in [:stores store-id :tickets ticket-id :status] :called)
                              (assoc-in [:stores store-id :tickets ticket-id :called-at] now))]
            {:state new-state
             :ticket (get-in new-state [:stores store-id :tickets ticket-id])}))))))

(defn- transition-ticket [state store-id ticket-id from to]
  (let [store (get-in state [:stores store-id])
        ticket (get-in store [:tickets ticket-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (when-not ticket (throw (err 404 :ticket-not-found "ticket not found")))
    (when-not (= from (:status ticket))
      (throw (err 409 :invalid-transition (str "ticket is " (name (:status ticket))))))
    (let [new-state (assoc-in state [:stores store-id :tickets ticket-id :status] to)]
      {:state new-state
       :ticket (get-in new-state [:stores store-id :tickets ticket-id])})))

(defn seat [state store-id ticket-id] (transition-ticket state store-id ticket-id :called :seated))
(defn no-show [state store-id ticket-id] (transition-ticket state store-id ticket-id :called :no-show))
(defn cancel [state store-id ticket-id] (transition-ticket state store-id ticket-id :waiting :cancelled))

(defn set-queue-status [state store-id tt-k status]
  (let [store (get-in state [:stores store-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (when-not (get-in store [:queues tt-k]) (throw (err 404 :table-type-not-found "table type not found")))
    (let [new-state (assoc-in state [:stores store-id :queues tt-k :status] status)]
      {:state new-state
       :queue (get-in new-state [:stores store-id :queues tt-k])})))

(defn status-by-token [state store-id token]
  (let [store (get-in state [:stores store-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (let [ticket-id (get-in store [:by-token token])]
      (when-not ticket-id (throw (err 404 :ticket-not-found "token not found")))
      (let [ticket (get-in store [:tickets ticket-id])
            tt (:table-type ticket)
            order (get-in store [:queues tt :order])
            pos (count (take-while #(not= ticket-id %) order))
            parties-ahead (count (filter #(= :waiting (get-in store [:tickets % :status]))
                                         (take pos order)))
            current-called (->> (reverse order)
                                (filter #(contains? #{:called :seated :no-show}
                                                    (get-in store [:tickets % :status])))
                                first
                                (#(get-in store [:tickets % :id])))]
        {:number (:id ticket)
         :token token
         :table-type (name tt)
         :label (get-in store [:table-types tt :label])
         :party-size (:party-size ticket)
         :status (:status ticket)
         :parties-ahead parties-ahead
         :current-called current-called}))))

(defn ticket-view [ticket]
  {:number (:id ticket)
   :party-size (:party-size ticket)
   :source (:source ticket)
   :status (:status ticket)})

(defn list-queue [state store-id tt-k]
  (let [store (get-in state [:stores store-id])]
    (when-not store (throw (err 404 :store-not-found "store not found")))
    (let [tt-v (get-in store [:table-types tt-k])
          q (get-in store [:queues tt-k])]
      (when-not tt-v (throw (err 404 :table-type-not-found "table type not found")))
      {:table-type (name tt-k)
       :label (:label tt-v)
       :status (:status q)
       :tickets (mapv #(ticket-view (get-in store [:tickets %])) (:order q))})))
