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

(defn close-open-periods
  [periods projection-model age-out-model age-out-projection-model seed]
  (println "Closing open periods...")
  (->> (for [{:keys [period-id beginning open? admission-age birthday duration] :as period} periods]
         (if open?
           (let [[s1 s2] (split seed)
                 current-duration duration
                 age-out? (age-out-model admission-age current-duration s1)]
             (loop [iter 1
                    seed s2]
               (if-let [{:keys [episodes-edn duration]} (if age-out?
                                                          (or (age-out-projection-model period-id seed)
                                                              (projection-model period-id seed))
                                                          (projection-model period-id seed))]
                 (if (and (< duration current-duration) (< iter 1000))
                   (recur (inc iter) (next-seed s2))
                   (let [duration (if (or age-out? (>= (time/year-interval birthday (time/days-after beginning duration)) 17))
                                    ;; Either we wanted to age out, or they did by virtue of staying beyond 17th birthday
                                    (periods/max-duration period)
                                    (min duration (periods/max-duration period)))]
                     (assoc period
                            :episodes (read-string episodes-edn)
                            :duration duration
                            :end (time/days-after beginning duration)
                            :provenance "P")))
                 ;; If we can't close a case, it's almost certainly
                 ;; because they are an aged-out case. Set max duration
                 (let [duration (periods/max-duration period)]
                   (assoc period
                          :duration duration
                          :end (time/days-after beginning duration)
                          :provenance "P")))))
           (assoc period :provenance "H")))
       #_(keep identity)))
