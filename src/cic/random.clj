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
    (map (fn [{:keys [beginning reported dob end] :as period} rng]
           (let [ ;; Earliest possible birthday is either January 1st in the year of their birth
                 ;; or 18 years prior to their final end date (or current report date if not yet ended),
                 ;; whichever is the later
                 earliest-birthday (time/latest (time/years-before (or end reported) 18)
                                                (time/make-date dob 1 1))
                 ;; Latest possible birthday is either December 31st in the year of their birth
                 ;; or the date they were taken into care, whichever is the sooner
                 latest-birthday (time/earliest beginning
                                                (time/make-date dob 12 31))
                 ;; True birthday must be somewhere between earliest and latest birthdays inclusive.
                 ;; Assume uniform distribution between the two.
                 birthday-offset (-> {:a 0 :b (time/day-interval earliest-birthday latest-birthday)}
                                     (d/uniform)
                                     (p/sample-1 rng))
                 birthday (time/days-after earliest-birthday birthday-offset)]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (time/year-interval birthday beginning)))))
         periods rngs)))
