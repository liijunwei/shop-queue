(ns shop-queue.storage
  "SQLite-backed storage. The whole store map is serialized as EDN into a single
  document row per store, so the persisted value is a 1:1 image of the domain
  value (keywords and strings round-trip faithfully).

  No SQL JOINs: one row per store, assembled in memory. When a store's document
  grows too large, split into normalized tables (tickets / queues / table_types)
  and load with separate per-store SELECTs, still assembling the store map in
  memory -- never a JOIN. `domain.clj` only sees the assembled map, so this swap
  stays inside `load`/`save-store!`."
  (:require [clojure.edn :as edn]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(defn connect [path]
  ;; WAL keeps readers working while the single writer is active; busy_timeout
  ;; makes a locked database wait briefly instead of failing immediately. The
  ;; driver applies these on every connection it opens (journal_mode persists
  ;; in the database file, busy_timeout is per connection).
  (jdbc/get-datasource
   {:jdbcUrl (str "jdbc:sqlite:" path "?journal_mode=WAL&busy_timeout=5000")}))

(defn init! [ds]
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS stores (store_id TEXT PRIMARY KEY, data TEXT NOT NULL)"])
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS operations (
                     store_id TEXT NOT NULL,
                     seq INTEGER NOT NULL,
                     data TEXT NOT NULL,
                     PRIMARY KEY (store_id, seq))"])
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS healthz (id INTEGER PRIMARY KEY, ts INTEGER NOT NULL)"])
  ds)

(defn writable?
  "Return true when the database accepts a write. Performs a heartbeat upsert
  on the `healthz` table, so it fails when the database is read-only or locked."
  [ds]
  (try
    (jdbc/execute! ds
      ["INSERT INTO healthz (id, ts) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET ts = excluded.ts"
       (System/currentTimeMillis)])
    true
    (catch Exception _ false)))

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

(defn append-operation!
  "Append one operation record to the append-only operations table and return
  the record with its :store-id and monotonically increasing :seq attached.
  Callers must serialize writes for the same store (the API write-lock does)."
  [ds store-id record]
  (let [seq (-> (jdbc/execute! ds
                 ["SELECT COALESCE(MAX(seq), -1) + 1 AS n
                  FROM operations WHERE store_id = ?" store-id]
                 {:builder-fn rs/as-unqualified-kebab-maps})
                first
                :n)]
    (jdbc/execute! ds
                   ["INSERT INTO operations (store_id, seq, data) VALUES (?, ?, ?)"
                    store-id seq (pr-str record)])
    (assoc record :store-id store-id :seq seq)))

(defn load-operations [ds store-id]
  (->> (jdbc/execute! ds
          ["SELECT store_id, seq, data FROM operations WHERE store_id = ? ORDER BY seq"
           store-id]
          {:builder-fn rs/as-unqualified-kebab-maps})
       (mapv (fn [{:keys [store-id seq data]}]
               (assoc (edn/read-string data) :store-id store-id :seq seq)))))
