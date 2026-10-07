(ns shop-queue.deploy
  "远程一键部署：`lein deploy`（等价于 `make deploy`）。"
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

(def uberjar  "target/uberjar/shop-queue-0.1.0-SNAPSHOT-standalone.jar")
(def ssh-host "tencent-openclaw")
(def remote-dir "/opt/shop-queue")
(def remote-jar "shop-queue-standalone.jar")

(defn- commands
  "返回要按顺序执行的命令（每个是一条由 argv 组成的向量）。"
  []
  [["scp" uberjar (str ssh-host ":/tmp/" remote-jar)]
   ["ssh" ssh-host (format "sudo mv /tmp/%s %s/%s && sudo systemctl restart shop-queue"
                           remote-jar remote-dir remote-jar)]
   ["ssh" ssh-host "curl -s http://localhost:8080/healthz"]])

(defn- run-cmd!
  "执行一条命令并打印输出；非零退出码时终止。"
  [[bin & args :as cmd]]
  (println "==>" (str/join " " cmd))
  (let [{:keys [exit out err]} (apply sh bin args)]
    (when (seq out) (print out))
    (when (seq err) (print err))
    (when (and exit (not= exit 0))
      (println "failed, exit=" exit)
      (System/exit exit))))

(defn -main [& args]
  (if (some #{"--dry"} args)
    (doseq [c (commands)] (println "(dry)" (str/join " " c)))
    (doseq [c (commands)] (run-cmd! c)))
  (println))
