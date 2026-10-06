(ns shop-queue.core
  (:gen-class)
  (:require [shop-queue.api :as api]))

(defn -main
  [& _]
  (api/start! 3000)
  (println "shop-queue listening on http://localhost:3000")
  (flush)
  @(promise))
