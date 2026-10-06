(ns shop-queue.viz
  "Pure renderer: turn a simulation trace into one self-contained HTML page with
  summary statistics, a per-ticket gantt chart and a per-table-type queue curve.
  The renderer depends only on the trace `:events`, not on any mutable state."
  (:require [clojure.string :as str]))

(def table-types [:two :three :large])
(def tt-label {:two "2人" :three "3人" :large "多人"})
(def tt-color {:two "#3b82f6" :three "#f59e0b" :large "#8b5cf6"})

(defn- avg [xs]
  (if (seq xs) (/ (reduce + xs) (count xs)) 0))

(defn- max-or-zero [xs]
  (if (seq xs) (apply max xs) 0))

(defn lifecycles
  "Map ticket-id -> {:id :created :called :final :status :table-type :party-size
  :n :source}. :status is :seat or :no-show once the ticket is finalized."
  [events]
  (reduce
   (fn [m e]
     (case (:type e)
       :created (assoc m (:ticket-id e)
                       {:id (:ticket-id e)
                        :created (:ts e)
                        :table-type (:table-type e)
                        :party-size (:party-size e)
                        :n (:n e)
                        :source (:source e)
                        :called nil :final nil :status nil})
       :called (assoc-in m [(:ticket-id e) :called] (:ts e))
       :seated (-> m
                   (assoc-in [(:ticket-id e) :final] (:ts e))
                   (assoc-in [(:ticket-id e) :status] :seat))
       :no-show (-> m
                    (assoc-in [(:ticket-id e) :final] (:ts e))
                    (assoc-in [(:ticket-id e) :status] :no-show))
       m))
   {}
   events))

(defn- queue-series
  "Returns [{:ts t :waiting {tt n} :load {tt n}} ...] at each distinct event
  timestamp (post-event), with an initial t=0 all-zero snapshot. `:waiting` is
  created-but-not-called; `:load` is created-but-not-finalized (waiting plus the
  in-flight called ticket)."
  [events]
  (let [tss (->> (map :ts events) distinct sort)
        by-ts (group-by :ts events)]
    (loop [tss tss
           created {} called {} finalized {}
           out [{:ts 0 :waiting {} :load {}}]]
      (if-let [t (first tss)]
        (let [es (get by-ts t)
              bump (fn [m pred]
                     (reduce (fn [acc e]
                               (if (pred (:type e))
                                 (update acc (:table-type e) (fnil inc 0))
                                 acc))
                             m es))
              created' (bump created #(= :created %))
              called' (bump called #(= :called %))
              finalized' (bump finalized #(or (= :seated %) (= :no-show %)))
              waiting (into {} (map (fn [tt] [tt (- (get created' tt 0) (get called' tt 0))]) table-types))
              load (into {} (map (fn [tt] [tt (- (get created' tt 0) (get finalized' tt 0))]) table-types))]
          (recur (rest tss) created' called' finalized'
                 (conj out {:ts t :waiting waiting :load load})))
        out))))

(defn summarize [events]
  (let [lcs (vals (lifecycles events))
        series (queue-series events)
        called (filter :called lcs)
        finalized (filter :final lcs)
        waits (map #(- (:called %) (:created %)) called)
        services (map #(- (:final %) (:called %)) finalized)
        no-shows (filter #(= :no-show (:status %)) finalized)
        waiting-total (fn [snap] (reduce + 0 (vals (:waiting snap))))
        by-tt (into (sorted-map)
                    (for [tt table-types]
                      (let [ts (filter #(= tt (:table-type %)) lcs)
                            waits' (map #(- (:called %) (:created %)) (filter :called ts))]
                        [tt {:count (count ts)
                             :avg-wait-min (avg waits')}])))]
    {:parties (count lcs)
     :max-queue (max-or-zero (map waiting-total series))
     :avg-wait-min (avg waits)
     :max-wait-min (max-or-zero waits)
     :avg-service-min (avg services)
     :no-show-count (count no-shows)
     :by-table-type by-tt}))

(defn- shade [hex f]
  (let [c (Long/parseLong (subs hex 1) 16)
        r (bit-and (bit-shift-right c 16) 255)
        g (bit-and (bit-shift-right c 8) 255)
        b (bit-and c 255)
        mix (fn [x] (int (+ (* f x) (* (- 1 f) 255))))]
    (format "#%02x%02x%02x" (mix r) (mix g) (mix b))))

(defn- gantt-svg [events]
  (let [tickets (->> (vals (lifecycles events)) (sort-by :created))
        tmax (max 1 (max-or-zero (map :ts events)))
        left 70 right 20 top 24 bottom 36
        plot-w (- 860 left right)
        row-h 13 gap 3
        h (+ top bottom (* (count tickets) (+ row-h gap)))
        x (fn [t] (+ left (* (/ t tmax) plot-w)))]
    (str
     "<svg xmlns='http://www.w3.org/2000/svg' width='860' height='" h "' viewBox='0 0 860 " h "'>"
     "<style>text{font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:11px} .tick{stroke:#e5e7eb}</style>"
     (apply str
            (for [m (range 0 (inc (long (Math/ceil tmax))) 10)]
              (str "<line class='tick' x1='" (x m) "' y1='" top "' x2='" (x m) "' y2='" (- h bottom) "'/>"
                   "<text x='" (x m) "' y='" (+ h -14) "' text-anchor='middle' fill='#6b7280'>" m "</text>")))
     (apply str
            (map-indexed
             (fn [i t]
               (let [y (+ top (* i (+ row-h gap)))
                     arrive (:created t)
                     call (:called t)
                     done (:final t)
                     col (tt-color (:table-type t))
                     waiting-end (if call call tmax)
                     svc-end (if done done tmax)]
                 (str
                  "<text x='" (- left 6) "' y='" (+ y 9) "' text-anchor='end' fill='#6b7280'>" (:id t) "</text>"
                  "<rect x='" (x arrive) "' y='" y "' width='" (max 1 (- (x waiting-end) (x arrive))) "' height='" row-h "' rx='2' fill='" (shade col 0.75) "'/>"
                  (when call
                    (str "<rect x='" (x call) "' y='" y "' width='" (max 1 (- (x svc-end) (x call))) "' height='" row-h "' rx='2' fill='" col "'/>"))
                  (when done
                    (let [dot (if (= :seat (:status t)) "#16a34a" "#dc2626")]
                      (str "<circle cx='" (x done) "' cy='" (+ y (/ row-h 2.0)) "' r='4' fill='" dot "'/>"))))))
             tickets))
     "</svg>")))

(defn- step-points [series tt tmax]
  (let [snaps (vec series)
        n (count snaps)]
    (loop [i 0 prev-val 0 pts []]
      (if (< i n)
        (let [t (:ts (nth snaps i))
              v (get-in (nth snaps i) [:load tt] 0)]
          (if (zero? i)
            (recur (inc i) v (conj pts [t v]))
            (recur (inc i) v (conj pts [t prev-val] [t v]))))
        (conj pts [tmax prev-val])))))

(defn- queue-svg [events]
  (let [series (queue-series events)
        tmax (max 1 (max-or-zero (map :ts events)))
        max-load (max 1 (max-or-zero (map #(reduce + 0 (vals (:load %))) series)))
        left 40 right 16 top 16 bottom 30
        w 860 h 220
        plot-w (- w left right) plot-h (- h top bottom)
        x (fn [t] (+ left (* (/ (double t) tmax) plot-w)))
        y (fn [q] (+ top (- plot-h (* (/ (double q) max-load) plot-h))))
        poly (fn [tt]
               (apply str
                      (map (fn [[px py]] (str px "," py " "))
                           (step-points series tt tmax))))]
    (str
     "<svg xmlns='http://www.w3.org/2000/svg' width='" w "' height='" h "' viewBox='0 0 " w " " h "'>"
     "<style>text{font-family:-apple-system,Segoe UI,Roboto,sans-serif;font-size:11px} .tick{stroke:#e5e7eb}</style>"
     (apply str
            (for [m (range 0 (inc (long (Math/ceil tmax))) 10)]
              (str "<line class='tick' x1='" (x m) "' y1='" top "' x2='" (x m) "' y2='" (+ top plot-h) "'/>")))
     (apply str
            (for [q (range 0 (inc max-load))]
              (str "<line class='tick' x1='" left "' y1='" (y q) "' x2='" (+ left plot-w) "' y2='" (y q) "'/>"
                   "<text x='" (- left 4) "' y='" (+ (y q) 3) "' text-anchor='end' fill='#6b7280'>" q "</text>")))
     (apply str
            (for [tt table-types]
              (str "<polyline fill='none' stroke='" (tt-color tt) "' stroke-width='1.6' points='" (poly tt) "'/>")))
     "<text x='" left "' y='" (+ h -8) "' fill='#6b7280'>队列长度随时间（分钟，含当前在途号）</text>"
     "</svg>")))

(defn- summary-html [events meta]
  (let [s (summarize events)
        row (fn [[tt {:keys [count avg-wait-min]}]]
              (str "<tr><td>" (tt-label tt) "</td><td>" count "</td><td>" (format "%.1f" avg-wait-min) "</td></tr>"))]
    (str
     "<p class='meta'>seed = " (get meta :seed)
     " · 假设：每桌型同时仅 1 桌（沿用当前 domain 的 in-flight 约束）</p>"
     "<table><thead><tr><th>桌型</th><th>取号数</th><th>平均等待(分)</th></tr></thead><tbody>"
     (apply str (map row (:by-table-type s)))
     "</tbody></table>"
     "<p>总取号 <b>" (:parties s)
     "</b> 单 · 峰值排队 <b>" (:max-queue s)
     "</b> · 平均等待 <b>" (format "%.1f" (:avg-wait-min s))
     "</b> 分 · 最长等待 <b>" (format "%.1f" (:max-wait-min s))
     "</b> 分 · 平均服务 <b>" (format "%.1f" (:avg-service-min s))
     "</b> 分 · 过号 <b>" (:no-show-count s) "</b></p>")))

(defn render
  "Render a trace (the map returned by `shop-queue.sim/run`) into a single
  self-contained HTML page."
  [trace]
  (let [events (:events trace)
        meta (:meta trace)]
    (str
     "<!doctype html><html lang='zh'><head><meta charset='utf-8'>"
     "<meta name='viewport' content='width=device-width,initial-scale=1'>"
     "<title>shop-queue 仿真可视化</title>"
     "<style>body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;color:#111827;max-width:900px}"
     "h1{font-size:20px}h2{font-size:15px;margin-top:28px}svg{display:block;margin-top:8px}"
     "table{border-collapse:collapse;margin:12px 0}th,td{border:1px solid #e5e7eb;padding:5px 12px;text-align:right}"
     "th:first-child,td:first-child{text-align:left}.meta{color:#6b7280;font-size:13px}</style></head><body>"
     "<h1>shop-queue 仿真：一次运行的回放</h1>"
     (summary-html events meta)
     "<h2>每个号码的生命周期甘特图（等待 → 叫号 → 入座/过号）</h2>"
     (gantt-svg events)
     "<h2>各桌型队列长度随时间变化</h2>"
     (queue-svg events)
     "</body></html>")))
