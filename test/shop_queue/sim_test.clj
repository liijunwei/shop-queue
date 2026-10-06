(ns shop-queue.sim-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [shop-queue.sim :as sim]
            [shop-queue.viz :as viz]))

(defn- created-events [events] (filter #(= :created (:type %)) events))
(defn- final-events [events] (filter #(#{:seated :no-show} (:type %)) events))

(deftest run-is-deterministic
  (let [a (sim/run {})
        b (sim/run {})]
    (is (= (:events a) (:events b)))
    (is (= (viz/render a) (viz/render b)))))

(deftest seed-changes-the-trace
  (is (not= (:events (sim/run {:seed 42 :parties 40}))
            (:events (sim/run {:seed 43 :parties 40})))))

(deftest drains-every-party
  (let [events (:events (sim/run {:seed 42 :parties 60}))
        created (set (map :ticket-id (created-events events)))
        finalized (set (map :ticket-id (final-events events)))]
    (is (= 60 (count created)))
    (is (= created finalized))))

(deftest event-order-contract
  (let [lcs (viz/lifecycles (:events (sim/run {:parties 60})))]
    (doseq [[id lc] lcs]
      (testing id
        (is (number? (:created lc)))
        (is (or (nil? (:called lc)) (<= (:created lc) (:called lc))))
        (is (or (nil? (:final lc)) (<= (or (:called lc) 0) (:final lc))))))))

(deftest render-output
  (let [html (viz/render (sim/run {:seed 42 :parties 30}))]
    (is (str/includes? html "<!doctype html>"))
    (is (= 2 (count (re-seq #"<svg" html))))
    (is (str/includes? html "甘特图"))
    (is (str/includes? html "队列长度"))
    (is (str/includes? html "总取号"))))

(deftest summarize-consistency
  (let [events (:events (sim/run {:seed 42 :parties 60}))
        s (viz/summarize events)]
    (is (= 60 (:parties s)))
    (is (pos? (:max-queue s)))
    (is (<= 0 (:no-show-count s) 60))
    (is (= 60 (reduce + (map :count (vals (:by-table-type s))))))))
