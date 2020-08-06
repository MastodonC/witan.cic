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

(defn sample-birthdays
  "We impute an arbitrary birthday for each child within their imputed 'birthday bounds'.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input."
  [periods seed]
  (let [rngs (split-n seed (count periods))]
    (mapv (fn [{:keys [beginning reported birth-month end period-id birthday-bounds] :as period} rng]
            (let [[earliest-birthday latest-birthday] birthday-bounds]
              (let [birthday-offset (-> {:a 0 :b (time/day-interval earliest-birthday latest-birthday)}
                                        (d/uniform)
                                        (p/sample-1 rng))
                    birthday (time/days-after earliest-birthday birthday-offset)]
                (-> period
                    (assoc :birthday birthday)
                    (assoc :admission-age (time/year-interval birthday beginning))))))
          periods rngs)))

(defn close-open-periods
  [periods knn-closed-cases]
  (let [closed-periods (remove :open? periods)
        closed-periods (zipmap (map :period-id closed-periods)
                               closed-periods)]
    (->> (for [period periods]
           (if (:open? period)
             (let [knn-closed-period (first (get knn-closed-cases (:period-id period)))
                   closed-period (get closed-periods (:closed knn-closed-period))]
               (if closed-period
                 (let [closed-offset (:offset knn-closed-period)
                       future-episodes (drop-while #(< (:offset %) closed-offset) (:episodes closed-period))
                       future-offset (- (:duration closed-period) closed-offset)
                       end (time/earliest (time/days-after (:beginning period) (+ (:duration period) future-offset))
                                          (time/years-after (:birthday period) 18))]
                   (-> period
                       (update :episodes concat future-episodes)
                       (assoc :duration (time/day-interval (:birthday period) end))
                       (assoc :end end))
                   )
                 (do (println (str "Can't close open case " (:period-id period) ", ignoring")))))
             period))
         (keep identity))))
