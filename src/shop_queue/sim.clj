(ns shop-queue.sim
  "Deterministic discrete-event simulation that drives the pure domain
  functions directly. It never touches HTTP or SQLite: a seeded RNG generates
  arrivals and service durations, and the ordinary domain commands produce the
  store transitions. Output is a time-ordered `trace` of every ticket's
  lifecycle event, ready for `shop-queue.viz/render`."
  (:require [clojure.string :as str]
            [shop-queue.domain :as domain]
            [shop-queue.viz :as viz]))

(def table-types [:two :three :large])

(def default-config
  {:seed 42
   :parties 60
   :inter-arrival-mean 8.0
   :service-mean {:two 9.0 :three 12.0 :large 16.0}
   :no-show-rate 0.10
   :party-size-weights {:two 0.5 :three 0.25 :large 0.25}
   :remote-rate 0.7})

(defn- exponential [rng mean]
  (* (double mean)
     (- (Math/log (max 1e-9 (- 1.0 (.nextDouble rng)))))))

(defn- service-mean [config tt]
  (let [sm (:service-mean config)]
    (double (if (map? sm) (get sm tt) sm))))

(defn- sample-party-size [rng weights]
  (let [weights (merge {:two 0.5 :three 0.25 :large 0.25} weights)
        total (double (reduce + (vals weights)))
        r (* (.nextDouble rng) total)]
    (loop [acc 0.0
           [[tt p] & more] (seq weights)]
      (let [acc' (+ acc (double p))]
        (if (or (< r acc') (nil? more))
          (case tt
            :two 2
            :three 3
            :large (+ 4 (.nextInt rng 3)))
          (recur acc' more))))))

(defn- sample-source [rng remote-rate]
  (if (< (.nextDouble rng) (double remote-rate)) "remote" "scan"))

(defn- next-event [pending]
  (first (sort-by (juxt :t :seq) pending)))

(defn- pop-event [pending ev]
  (vec (remove #(identical? % ev) pending)))

(defn- try-serve
  "For each table type, call the next waiting ticket when no ticket is currently
  :called (the current single-table-per-type rule), emit a :called event and
  schedule the serve-done event."
  [st config rng]
  (loop [store (:store st)
         pending (:pending st)
         trace (:trace st)
         next-seq (:next-seq st)
         tts table-types]
    (if-let [tt (first tts)]
      (if (and (nil? (domain/current-called store tt))
               (seq (domain/waiting-tickets store tt)))
        (let [res (domain/call-next store tt (:clock st))]
          (if (:ok res)
            (let [id (:id (:value res))
                  outcome (if (< (.nextDouble rng) (double (:no-show-rate config)))
                            :no-show
                            :seat)
                  dur (exponential rng (service-mean config tt))]
              (recur (:state res)
                     (conj pending {:t (+ (:clock st) dur)
                                    :kind :serve-done :seq next-seq
                                    :table-type tt :ticket-id id :outcome outcome})
                     (conj trace {:ts (:clock st) :type :called
                                  :table-type tt :ticket-id id})
                     (inc next-seq)
                     (rest tts)))
            (recur store pending trace next-seq (rest tts))))
        (recur store pending trace next-seq (rest tts)))
      (assoc st :store store :pending pending :trace trace :next-seq next-seq))))

(defn run
  "Run a deterministic simulation. Returns
  {:meta {:seed .. :config ..} :events [...]}. `:events` are time-ordered maps of
  shape {:ts .. :type .. :table-type .. :ticket-id ..}, where :type is one of
  :created / :called / :seated / :no-show."
  ([config]
   (let [config (merge default-config config)]
     (run config (java.util.Random. (long (:seed config))))))
  ([config rng]
   (let [config (merge default-config config)
         parties (long (:parties config))]
     (if (<= parties 0)
       {:meta {:seed (:seed config) :config config} :events []}
       (loop [st {:clock 0.0
                  :store (domain/seed-store {})
                  :pending [{:t 0.0 :kind :arrival :seq 0}]
                  :trace []
                  :arrivals 0
                  :next-seq 1}]
         (if (and (>= (:arrivals st) parties) (empty? (:pending st)))
           {:meta {:seed (:seed config) :config config}
            :events (vec (:trace st))}
           (let [ev (next-event (:pending st))
                 st (assoc st :clock (:t ev)
                              :pending (pop-event (:pending st) ev))]
             (case (:kind ev)
               :arrival
               (let [ps (sample-party-size rng (:party-size-weights config))
                     src (sample-source rng (:remote-rate config))
                     arrivals' (inc (:arrivals st))
                     id (format "T%04d" arrivals')
                     token (str "sim-" id)
                     res (domain/create-ticket (:store st) ps src (:clock st) token id)]
                 (if (:ok res)
                   (let [new-store (:state res)
                         created {:ts (:clock st) :type :created
                                  :table-type (:table-type (:value res))
                                  :ticket-id id
                                  :n (get-in new-store [:tickets id :n])
                                  :party-size ps :source src}
                         st (assoc st :store new-store
                                      :arrivals arrivals'
                                      :trace (conj (:trace st) created))
                         st (if (< arrivals' parties)
                              (-> st
                                  (update :pending conj
                                          {:t (+ (:clock st)
                                                 (exponential rng (double (:inter-arrival-mean config))))
                                           :kind :arrival :seq (:next-seq st)})
                                  (update :next-seq inc))
                              st)
                         st (try-serve st config rng)]
                     (recur st))
                   (recur st)))

               :serve-done
               (let [{:keys [table-type ticket-id outcome]} ev
                     f (if (= :seat outcome) domain/seat domain/no-show)
                     res (f (:store st) table-type ticket-id (:clock st))
                     st (if (:ok res)
                          (assoc st
                                 :store (:state res)
                                 :trace (conj (:trace st)
                                              {:ts (:clock st)
                                               :type (if (= :seat outcome) :seated :no-show)
                                               :table-type table-type
                                               :ticket-id ticket-id}))
                          st)
                     st (try-serve st config rng)]
                 (recur st))))))))))

(defn- str->long [s] (Long/parseLong s))
(defn- str->double [s] (Double/parseDouble s))

(defn- parse-service-mean [s]
  (if (str/includes? s "=")
    (into {}
          (for [kv (str/split s #",")
                :let [[k v] (str/split kv #"=" 2)]]
            [(keyword k) (str->double v)]))
    (str->double s)))

(defn- parse-weights [s]
  (let [[p2 p3 pl] (map str->double (str/split s #","))]
    {:two p2 :three p3 :large pl}))

(defn- parse-args [args]
  (let [args (if (= "--" (first args)) (rest args) args)]
    (loop [args (vec args)
           config {}
           out "sim-output.html"]
      (if (empty? args)
        {:config config :out out}
        (let [k (first args)
              v (second args)
              args (nthrest args 2)]
          (case k
            "--seed" (recur args (assoc config :seed (str->long v)) out)
            "--parties" (recur args (assoc config :parties (str->long v)) out)
            "--inter-arrival-mean" (recur args (assoc config :inter-arrival-mean (str->double v)) out)
            "--service-mean" (recur args (assoc config :service-mean (parse-service-mean v)) out)
            "--no-show-rate" (recur args (assoc config :no-show-rate (str->double v)) out)
            "--party-size-weights" (recur args (assoc config :party-size-weights (parse-weights v)) out)
            "--out" (recur args config v)
            (recur args config out)))))))

(defn -main [& args]
  (let [{:keys [config out]} (parse-args args)
        trace (run config)
        s (viz/summarize (:events trace))]
    (spit out (viz/render trace))
    (println (format "seed=%d parties=%d max-queue=%d avg-wait=%.1f avg-service=%.1f no-show=%d -> %s"
                     (:seed (:meta trace))
                     (:parties s)
                     (:max-queue s)
                     (:avg-wait-min s)
                     (:avg-service-min s)
                     (:no-show-count s)
                     out))))
