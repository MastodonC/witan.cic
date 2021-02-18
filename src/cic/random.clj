(ns cic.random
  (:refer-clojure :exclude [rand-nth])
  (:require [cic.time :as time]
            [cic.periods :as periods]
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
  (when (seq coll)
    (let [i (int (p/sample-1 (d/uniform {:a 0 :b (count coll)}) seed))]
      (nth coll i))))

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

(defn close-open-periods
  [periods projection-model age-out-model age-out-projection-model seed]
  (println "Closing open periods...")
  (->> (for [{:keys [period-id beginning open? admission-age birthday] :as period} periods
             :let [age-out? (age-out-model admission-age seed)]]
         (if open?
           (when-let [{:keys [episodes-edn duration]} (if age-out?
                                                        (or (age-out-projection-model period-id)
                                                            (projection-model period-id))
                                                        (projection-model period-id))]
             (let [duration (min duration (periods/max-duration period))]
               (assoc period
                      :episodes (read-string episodes-edn)
                      :duration duration
                      :end (time/days-after beginning duration)
                      :provenance "P")))
           (assoc period :provenance "H")))
       (keep identity)))
