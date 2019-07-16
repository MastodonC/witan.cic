(ns cic.project
  (:require [clojure.test.check.random :as r]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]
            [tick.alpha.api :as t]
            [tick.core :as tick]))

(defn day-offset-in-year
  [date]
  (t/days (t/duration (t/new-interval (t/year date) date))))

(defn interval-duration
  [start stop]
  (t/duration (t/new-interval start stop)))

(defn prepare-ages
  "All we know about a child is their year of birth, so we impute an arbitrary birthday.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input.
  The only constraint besides their year of birth is that a child can't be a negative age at admission"
  [open-periods seed]
  (let [rngs (r/split-n (r/make-random seed) (count open-periods))]
    (map (fn [{:keys [beginning dob] :as period} rng]
           (let [base-date (t/new-date dob 1 1)
                 max-offset (day-offset-in-year beginning)
                 offset (-> (if (= (-> period :beginning t/year) (t/year dob))
                              (d/uniform 0 max-offset)
                              (d/uniform 0 365))
                            (p/sample-1 rng))
                 birthday (tick/forward-number base-date offset)]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (quot (t/days (interval-duration birthday beginning)) 365)))))
         open-periods rngs)))
