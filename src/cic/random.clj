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
  [periods knn-closed-cases seed]
  (let [closed-periods (remove :open? periods)
        closed-periods (zipmap (map :period-id closed-periods)
                               closed-periods)]
    (->> (for [period periods
               :let [open-offset (time/day-interval (:beginning period) (:reported period))]]
           (if (:open? period)
             (let [knn-closed-periods (map (fn [{:keys [closed offset]}]
                                             {:period (get closed-periods closed)
                                              :offset offset})
                                           (get knn-closed-cases (:period-id period))) ;; TODO use
                   knn-closed-period (or (some #(when (and (> (-> % :period :duration) open-offset))
                                                  %)
                                               (shuffle knn-closed-periods))
                                         (do (println (format "Didn't find period within offset: %s in placement %s" (:period-id period) (-> period :episodes last :placement)))
                                             (first (sort-by (comp :duration :period) > knn-closed-periods)))) ;; Take longest available duration
                   closed-period (:period knn-closed-period)]
               (if closed-period
                 (let [closed-offset (:offset knn-closed-period)
                       closed-duration (:duration closed-period)
                       future-duration (- closed-duration closed-offset)
                       end (time/earliest (time/days-after (:beginning period) (+ (:duration period) future-duration))
                                          (time/days-before (time/years-after (:birthday period) 18) 1))
                       simulated-duration (time/day-interval (:beginning period) end)
                       ;; Let's make sure the placements match up as they should
                       _ (when (not=
                                (:placement (last (:episodes period)))
                                (:placement (last (take-while #(< (:offset %) closed-offset) (:episodes closed-period)))))
                           (print period closed-period closed-offset (last (:episodes period)) (last (take-while #(< (:offset %) closed-offset) (:episodes closed-period)))))
                       future-episodes (->> (:episodes closed-period)
                                            (drop-while #(< (:offset %) closed-offset))
                                            (map (fn [{:keys [offset] :as episode}]
                                                   (-> episode
                                                       (update :offset - closed-offset)
                                                       (update :offset + open-offset))))
                                            (take-while #(< (:offset %) simulated-duration)))
                       period (-> period
                                  (assoc :open? false)
                                  (update :episodes concat future-episodes)
                                  (assoc :duration simulated-duration)
                                  (assoc :end end))]
                   period)
                 (do (println (str "Can't close open period " (:period-id period) ", ignoring")))))
             period))
         (keep identity))))
