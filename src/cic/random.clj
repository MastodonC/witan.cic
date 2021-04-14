(ns cic.random
  (:refer-clojure :exclude [rand-nth])
  (:require [cic.time :as time]
            [clojure.test.check.random :as r]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]))

(defn seed
  [seed]
  (r/make-random seed))

(def split r/split)

(def next-seed (comp first split))

(def split-n r/split-n)

(def rand-long r/rand-long)

(def rand-double r/rand-double)

(defn rand-nth
  [coll seed]
  ;; (assert vector? coll)
  (let [length (count coll)
        n (p/sample-1 (d/uniform {:a 0 :b length}) seed)]
    (nth coll n)))

(let [letters (map char (range 65 90))]
  (defn rand-id
    [n seed]
    (->> (r/split-n seed n)
         (map (partial rand-nth letters))
         (take n)
         (apply str))))

(defn sample-birthdays
  "We impute an arbitrary birthday for each child within their imputed 'birthday bounds'.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input."
  [periods seed]
  (let [rngs (split-n seed (count periods))]
    (mapv (fn [{:keys [beginning birth-month end period-id birthday-bounds] :as period} rng]
            (let [[earliest-birthday latest-birthday] birthday-bounds]
              (let [birthday-offset (-> {:a 0 :b (time/day-interval earliest-birthday latest-birthday)}
                                        (d/uniform)
                                        (p/sample-1 rng))
                    birthday (time/days-after earliest-birthday birthday-offset)]
                (-> period
                    (assoc :birthday birthday)
                    (assoc :admission-age (time/year-interval birthday beginning))
                    (assoc :admission-age-days (time/day-interval birthday beginning))))))
          periods rngs)))
