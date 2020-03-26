(ns cic.validate
  (:require [cic.periods :as periods]
            [cic.projection :as p]
            [cic.summary :as summary]
            [cic.time :as time]
            [clj-time.coerce :as coerce]
            [clj-time.core :as tc]
            [kixi.stats.core :as kixi]
            [kixi.stats.protocols :as kp]))

;; We want to generate a RMSE for a variety of input ranges

(defn linear-regression
  [periods predict]
  (let [from (apply tc/min-date (map :beginning periods))
        to (apply tc/max-date (keep :end periods))
        xs (time/day-seq from to 7)
        ys (map #(count (filter (periods/in-care? %) periods)) xs)
        xs (map coerce/to-long xs)
        model (transduce identity (kixi/simple-linear-regression first second) (map vector xs ys))
        x (coerce/to-long predict)]
    (long (kp/measure model x))))

(defn model
  [projection-seed model-seed predict {:keys [seed n-runs] :or {seed 42 n-runs 10}}]
  (let [placement-costs {} ;; Not required
        projections (->> (p/project-n projection-seed model-seed [predict] seed n-runs)
                         (map #(summary/periods-summary % [predict] placement-costs)))]
    (transduce (map (comp :count #(get % predict))) kixi/median projections)))

(defn actual
  [periods predict]
  (count (filter (periods/in-care? predict) periods)))

(defn compare-models-at [as-at duration-model placements-model periods seed n-runs]
  (let [periods-as-at (periods/periods-as-at periods as-at)
        learn-from (time/years-before as-at 2)
        project-to (time/years-after as-at 1)
        projection-seed {:seed (filter :open? periods-as-at)
                         :date as-at}
        model-seed {:seed periods
                    :duration-model duration-model
                    :placements-model placements-model
                    :joiner-range [learn-from as-at]
                    :episodes-range [learn-from as-at]}]
    (hash-map :date as-at
              :model (model projection-seed model-seed project-to {:seed seed :n-runs n-runs})
              :linear-regression (linear-regression periods-as-at project-to)
              :actual (actual periods project-to))))
