(ns shop-queue.core
  (:require [org.httpkit.server :as http]
            [shop-queue.api :as api]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage])
  (:gen-class))

(defn start-system!
  "Connect to `db-path`, initialize the schema and load the persisted store
  state. Returns the assembled system map, or throws when loading fails — it
  never returns a freshly seeded/empty state on a load error."
  [db-path]
  (let [ds (storage/init! (storage/connect db-path))]
    (try
      (let [loaded (storage/load-all ds)
            stores (if (seq loaded) loaded {"store-1" (domain/seed-store {})})
            seeded (into {}
                         (map (fn [[id s]] [id (domain/seed-store s)]))
                         stores)
            state (atom {:stores seeded})]
        ;; Persist only brand-new stores (a fresh seed); stores already in the
        ;; database keep their persisted :version so the optimistic CAS stays in
        ;; sync and startup never bumps versions pointlessly.
        (doseq [[id store] seeded
                :when (not (contains? loaded id))]
          (swap! state assoc-in [:stores id] (storage/save-store! ds id store)))
        {:db ds :state state
         :subs (atom {:by-token {} :by-table-type {}})
         :events (atom {:seq 0 :by-token {}})})
      (catch Exception e
        (throw (ex-info (str "failed to load store state from " (pr-str db-path)
                             "; refusing to start with empty state")
                        {:db-path db-path :cause (.getMessage e)}
                        e))))))

(defn -main [& _]
  (let [db-path (or (System/getenv "QUEUE_DB") "queue.db")
        port (Integer/parseInt (or (System/getenv "PORT") "8080"))]
    (try
      (let [system (start-system! db-path)]
        (println "shop-queue listening on" port "db=" db-path)
        (http/run-server (api/handler system) {:port port})
        @(promise))
      (catch Exception e
        (binding [*out* *err*]
          (println "FATAL: startup aborted; shop-queue is NOT serving.")
          (println (.getMessage e)))
        (System/exit 1)))))
