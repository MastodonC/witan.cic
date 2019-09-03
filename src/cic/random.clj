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

(def split-n r/split-n)

(def rand-long r/rand-long)

(defn rand-nth
  [coll seed]
  (let [i (int (p/sample-1 (d/uniform {:a 0 :b (count coll)}) seed))]
    (nth coll i)))

(let [letters (map char (range 65 90))]
  (defn rand-id
    [n seed]
    (->> (r/split-n seed n)
         (map (partial rand-nth letters))
         (take n)
         (apply str))))

(defn prepare-ages
  "All we know about a child is their year of birth, so we impute an arbitrary birthday.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input.
  The only constraint besides their year of birth is that a child can't be a negative age at admission"
  [open-periods seed]
  (let [rngs (split-n seed (count open-periods))]
    (map (fn [{:keys [beginning dob] :as period} rng]
           (let [base-date (time/make-date dob 1 1)
                 max-offset (time/day-offset-in-year beginning)
                 offset (-> (if (= (-> period :beginning time/year) dob)
                              (d/uniform {:a 0 :b max-offset})
                              (d/uniform {:a 0 :b 365}))
                            (p/sample-1 rng))
                 birthday (time/days-after base-date offset)]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (quot (time/day-interval birthday beginning) 365)))))
         open-periods rngs)))
