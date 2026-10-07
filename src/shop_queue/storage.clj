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
  ;; Concurrency model: in-process writes are serialized by the API write-lock;
  ;; cross-process writes are protected by the version CAS in `save-store!`,
  ;; which detects a concurrent modification instead of silently overwriting.
  (jdbc/get-datasource
   {:jdbcUrl (str "jdbc:sqlite:" path "?journal_mode=WAL&busy_timeout=5000")}))

(defn- ensure-version-column [ds]
  (let [cols (set (map :name (jdbc/execute! ds ["PRAGMA table_info(stores)"]
                                            {:builder-fn rs/as-unqualified-kebab-maps})))]
    (when-not (contains? cols "version")
      (jdbc/execute! ds ["ALTER TABLE stores ADD COLUMN version INTEGER NOT NULL DEFAULT 0"]))))

(defn init! [ds]
  (jdbc/execute! ds
                 ["CREATE TABLE IF NOT EXISTS stores (
                     store_id TEXT PRIMARY KEY,
                     data TEXT NOT NULL,
                     version INTEGER NOT NULL DEFAULT 0)"])
  (ensure-version-column ds)
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
  "Optimistically persist one store's whole document. `store`'s :version is the
  expected version; on success the document advances by one and the new store
  (with bumped :version) is returned. When another writer already advanced the
  version, throws an ExceptionInfo with {:code :store-conflict} so the caller can
  surface a 409 instead of silently overwriting."
  [ds store-id store]
  (let [expected (long (or (:version store) 0))
        next-version (inc expected)
        new-store (assoc store :version next-version)
        data (pr-str new-store)
        result (jdbc/execute! ds
                 ["INSERT INTO stores (store_id, data, version) VALUES (?, ?, ?)
                   ON CONFLICT(store_id) DO UPDATE SET
                     data = excluded.data,
                     version = excluded.version
                   WHERE stores.version = ?"
                  store-id data next-version expected])
        changed (long (or (:next.jdbc/update-count (first result)) 0))]
    (if (zero? changed)
      (throw (ex-info "store version conflict"
                      {:code :store-conflict :store-id store-id :expected expected}))
      new-store)))

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
