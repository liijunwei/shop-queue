(ns shop-queue.core
  (:require [org.httpkit.server :as http]
            [shop-queue.api :as api]
            [shop-queue.domain :as domain]
            [shop-queue.storage :as storage])
  (:gen-class))

(defn -main [& _]
  (let [db-path (or (System/getenv "QUEUE_DB") "queue.db")
        port (Integer/parseInt (or (System/getenv "PORT") "8080"))
        ds (storage/init! (storage/connect db-path))
        loaded (storage/load-all ds)
        stores (if (seq loaded) loaded {"store-1" (domain/seed-store {})})
        state (atom {:stores (into {} (map (fn [[id s]] [id (domain/seed-store s)])) stores)})
        system {:db ds :state state :subs (atom {:by-token {} :by-table-type {}})}]
    (doseq [[id store] (:stores @state)]
      (storage/save-store! ds id store))
    (println "shop-queue listening on" port "db=" db-path)
    (http/run-server (api/handler system) {:port port})
    @(promise)))
