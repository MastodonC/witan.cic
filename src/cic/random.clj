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
  Constraints:
  Year of birth must match the provided year
  A child can't be a negative age at admission
  A child must have left by the time they are 18"
  [periods seed]
  (let [rngs (split-n seed (count periods))]
    (map (fn [{:keys [beginning correct-at dob end] :as period} rng]
           (let [earliest-birthday (time/latest (time/days-after (time/years-before (or end correct-at) 18) 1)
                                                (time/make-date dob 1 1))
                 latest-birthday (time/earliest beginning
                                                (time/days-before (time/make-date (inc dob) 1 1) 1))
                 birthday-offset (-> {:a 0 :b (time/day-interval earliest-birthday latest-birthday)}
                                     (d/uniform)
                                     (p/sample-1 rng))
                 birthday (time/days-after earliest-birthday birthday-offset)]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (time/year-interval birthday beginning)))))
         periods rngs)))
