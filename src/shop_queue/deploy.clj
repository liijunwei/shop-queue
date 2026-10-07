(ns shop-queue.deploy
  "远程一键部署：`lein deploy`。"
  (:require [clojure.java.shell :refer [sh]]
            [clojure.string :as str]))

(def uberjar   "target/uberjar/shop-queue-0.1.0-SNAPSHOT-standalone.jar")
(def ssh-host  "tencent-openclaw")
(def remote-dir "/opt/shop-queue")
(def remote-jar "shop-queue-standalone.jar")

(defn- run-cmd!
  "执行命令、打印输出，返回 {:exit :out}。"
  [cmd]
  (println "==>" (str/join " " cmd))
  (let [{:keys [exit out err]} (apply sh cmd)]
    (when (seq out)
      (print out)
      (when-not (str/ends-with? out "\n") (println)))
    (when (seq err)
      (print err)
      (when-not (str/ends-with? err "\n") (println)))
    {:exit (or exit 0) :out out}))

(defn- required!
  "执行命令，非零退出码时终止。"
  [cmd]
  (let [{:keys [exit]} (run-cmd! cmd)]
    (when (not= exit 0)
      (println "failed, exit=" exit)
      (System/exit exit))))

(defn- verify-healthz!
  "systemctl restart 返回后服务还要 1~2s 才监听，轮询 /healthz 至多约 30s。"
  []
  (let [cmd ["ssh" ssh-host "curl -s -m 3 http://localhost:8080/healthz"]]
    (loop [attempt 1]
      (let [{:keys [exit out]} (run-cmd! cmd)]
        (if (and (= exit 0) (str/includes? (or out "") "\"status\":\"ok\""))
          (println "==> healthz ok")
          (if (>= attempt 10)
            (do (println "==> /healthz 仍不可达，请运行 systemctl status shop-queue 排查")
                (System/exit 1))
            (do (Thread/sleep 3000)
                (recur (inc attempt)))))))))

(defn -main [& args]
  (let [dry? (boolean (some #{"--dry"} args))
        scp  ["scp" uberjar (str ssh-host ":/tmp/" remote-jar)]
        mv   ["ssh" ssh-host (format "sudo mv /tmp/%s %s/%s && sudo systemctl restart shop-queue"
                                     remote-jar remote-dir remote-jar)]
        hc   ["ssh" ssh-host "curl -s -m 3 http://localhost:8080/healthz"]]
    (if dry?
      (do (println "(dry)" (str/join " " scp))
          (println "(dry)" (str/join " " mv))
          (println "(dry)" (str/join " " hc) "（重试至多 30s）"))
      (do (required! scp)
          (required! mv)
          (verify-healthz!)))))
