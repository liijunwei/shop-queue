(ns shop-queue.backup
  "CLI for taking a consistent SQLite snapshot with VACUUM INTO. Intended for
  daily/cron backups: the destination is derived from BACKUP_DIR plus a
  timestamp, so repeated runs never collide."
  (:require [clojure.java.io :as io]
            [shop-queue.storage :as storage])
  (:gen-class))

(defn- timestamp []
  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HHmmss") (java.util.Date.)))

(defn -main [& args]
  (let [db-path (or (first args) (System/getenv "QUEUE_DB") "queue.db")
        backup-dir (or (second args) (System/getenv "BACKUP_DIR"))]
    (cond
      (nil? backup-dir)
      (do (println "usage: lein run -m shop-queue.backup [QUEUE_DB] BACKUP_DIR")
          (println "       or set QUEUE_DB and BACKUP_DIR")
          (System/exit 2))

      (not (.exists (io/file db-path)))
      (do (println "backup failed: source database not found:" db-path)
          (System/exit 1))

      :else
      (try
        (let [dest (io/file backup-dir (str "queue-" (timestamp) ".db"))
              _ (io/make-parents dest)
              ds (storage/connect db-path)
              out (storage/backup! ds (str dest))]
          (println "backup written to" out)
          (System/exit 0))
        (catch Exception e
          (println "backup failed:" (.getMessage e))
          (System/exit 1))))))
