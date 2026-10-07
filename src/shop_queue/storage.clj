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
  ;;
  ;; Concurrency model: one writer process per database file. WAL + busy_timeout
  ;; smooth reader/writer contention inside that process, but do not make the
  ;; whole-document store updates safe across multiple processes (see
  ;; save-store!).
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
                 ["CREATE TABLE IF NOT EXISTS counters (
                     store_id TEXT NOT NULL,
                     table_type TEXT NOT NULL,
                     counter INTEGER NOT NULL DEFAULT 0,
                     PRIMARY KEY (store_id, table_type))"])
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS tokens (
                     token TEXT PRIMARY KEY,
                     store_id TEXT NOT NULL)"])
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

(defn save-store!
  "Persist one store's whole document. This is a last-writer-wins upsert of the
  entire store map, so it is correct only under the declared single-process,
  single-writer deployment (the API write-lock serializes writers in-process).
  Two OS processes sharing one database file can silently overwrite each other's
  store documents even though the counter/token tables are allocated atomically.
  Keep exactly one writer process per database file."
  [ds store-id store]
  (jdbc/execute! ds
                 ["INSERT INTO stores (store_id, data) VALUES (?, ?)
      ON CONFLICT(store_id) DO UPDATE SET data = excluded.data"
                  store-id (pr-str store)]))

(defn next-counter!
  "Atomically allocate the next sequential number for one store/table-type
  queue. `current` is the counter recorded in the store document. The single
  UPSERT first raises the stored counter to at least `current + 1`, then on
  conflict increments the existing value, and `RETURNING` hands back the result.
  This is one atomic SQLite statement, so concurrent callers get distinct,
  gap-free numbers without relying on an application lock."
  [ds store-id table-type current]
  (-> (jdbc/execute! ds
        ["INSERT INTO counters (store_id, table_type, counter) VALUES (?, ?, ?)
          ON CONFLICT(store_id, table_type) DO UPDATE SET
            counter = MAX(counters.counter + 1, excluded.counter)
          RETURNING counter"
         store-id (name table-type) (inc (long current))]
        {:builder-fn rs/as-unqualified-kebab-maps})
      first
      :counter))

(defn claim-token!
  "Claim `token` for `store-id`. The `tokens.token` primary key enforces global
  uniqueness; return true when the token was newly claimed and false when it
  already exists."
  [ds store-id token]
  (pos? (long (or (-> (jdbc/execute! ds
                          ["INSERT INTO tokens (token, store_id) VALUES (?, ?)
                            ON CONFLICT(token) DO NOTHING"
                           token store-id])
                      first
                      :next.jdbc/update-count)
                  0))))

(defn append-operation!
  "Append one operation record to the append-only operations table and return
  the record with its :store-id and monotonically increasing :seq attached.
  The seq is allocated by a single INSERT ... RETURNING statement, so concurrent
  callers for the same store get distinct, gap-free seqs without a separate
  read-then-insert race (SQLite serializes writers at the database level)."
  [ds store-id record]
  (let [seq (-> (jdbc/execute! ds
                 ["INSERT INTO operations (store_id, seq, data)
                   VALUES (?, COALESCE((SELECT MAX(seq) FROM operations WHERE store_id = ?), -1) + 1, ?)
                   RETURNING seq"
                  store-id store-id (pr-str record)]
                 {:builder-fn rs/as-unqualified-kebab-maps})
                first
                :seq)]
    (assoc record :store-id store-id :seq seq)))

(defn load-operations [ds store-id]
  (->> (jdbc/execute! ds
          ["SELECT store_id, seq, data FROM operations WHERE store_id = ? ORDER BY seq"
           store-id]
          {:builder-fn rs/as-unqualified-kebab-maps})
       (mapv (fn [{:keys [store-id seq data]}]
               (assoc (edn/read-string data) :store-id store-id :seq seq)))))

(defn backup!
  "Take a consistent, point-in-time snapshot of the whole SQLite database into
  `dest` using SQLite's VACUUM INTO. The destination must not already exist.
  Returns `dest` on success and throws on any failure."
  [ds dest]
  (jdbc/execute! ds ["VACUUM INTO ?" (str dest)])
  dest)
