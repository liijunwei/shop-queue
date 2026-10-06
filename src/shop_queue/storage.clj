(ns shop-queue.storage
  "SQLite-backed storage. The whole store map is serialized as EDN into a single
  document row per store, so the persisted value is a 1:1 image of the domain
  value (keywords and strings round-trip faithfully)."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(defn connect [path]
  (jdbc/get-datasource {:jdbcUrl (str "jdbc:sqlite:" path)}))

(defn init! [ds]
  (jdbc/execute! ds
    ["CREATE TABLE IF NOT EXISTS stores (store_id TEXT PRIMARY KEY, data TEXT NOT NULL)"])
  ds)

(defn load-all [ds]
  (into {}
        (map (fn [{:keys [store-id data]}]
               [store-id (edn/read-string data)]))
        (jdbc/execute! ds ["SELECT store_id, data FROM stores"]
                       {:builder-fn rs/as-unqualified-kebab-maps})))

(defn save-store! [ds store-id store]
  (jdbc/execute! ds
    ["INSERT INTO stores (store_id, data) VALUES (?, ?)
      ON CONFLICT(store_id) DO UPDATE SET data = excluded.data"
     store-id (pr-str store)]))
