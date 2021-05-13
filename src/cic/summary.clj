(ns cic.summary
  (:require [cic.time :as time]
            [cic.spec :as spec]
            [cic.periods :as periods]
            [net.cgrand.xforms :as x]
            [redux.core :as redux]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]))

(defn placement-age
  [placement age]
  (str (name placement) "-" age))

(defn periods-summary
  "Takes inferred future periods and calculates the total CiC"
  [periods dates]
  (let [placements-zero (zipmap spec/placements (repeat 0))
        ages-zero (zipmap spec/ages (repeat 0))]
    (reduce (fn [output date]
              (let [in-care (filter (periods/in-care? date) periods)
                    by-placement (->> (map #(:placement (periods/episode-on % date)) in-care)
                                      (frequencies))
                    by-age (->> (map #(periods/age-on % date) in-care)
                                (frequencies))]
                (assoc output date {:count (count in-care)
                                    :placements (merge-with + placements-zero by-placement)
                                    :ages (merge-with + ages-zero by-age)
                                    })))
            {} dates)))


(defn mjuxt
  "A version of juxt that accepts a map of keys to functions.
  Returns a function accepts a single argument and returns a map of keys to results."
  [kvs]
  (let [[ks fns] (apply map vector kvs)]
    (comp (partial zipmap ks) (apply juxt fns))))

(defn mapm
  "Like `map`, but returns a mapping from inputs to outputs."
  [f xs]
  (->> (map (juxt identity f) xs)
       (into {})))

(defn getter
  [k]
  (fn [x]
    (get x k)))

(defn rf-for-keys
  "A reducing function which will calculate the median over vals corresponding to keys."
  [rf keys]
  (-> (mapm #(redux/pre-step rf (getter %)) keys)
      (redux/fuse)))

(defn confidence-intervals
  [dist]
  {:lower (d/quantile dist 0.05)
   :q1 (d/quantile dist 0.25)
   :median (d/quantile dist 0.5)
   :q3 (d/quantile dist 0.75)
   :upper (d/quantile dist 0.95)})

(def histogram-rf
  (redux/post-complete k/histogram confidence-intervals))

(def combo-rf
  "A reducing function which will calculate data for each output row"
  (redux/fuse {:projected (redux/pre-step histogram-rf :count)
               :projected-cost (redux/pre-step histogram-rf :cost)
               :placements (-> (rf-for-keys histogram-rf spec/placements)
                               (redux/pre-step :placements))
               :ages (-> (rf-for-keys histogram-rf spec/ages)
                         (redux/pre-step :ages))}))

(defn grand-summary
  "A function to transduce over all runs.
  Assumes that the dates for all runs are the same.
  Calculates the summary statistics incrementally so that raw data needn't be held in memory."
  [runs]
  (let [dates (->> runs first keys)
        rf (->> (map (fn [date] (vector date (redux/pre-step combo-rf (getter date)))) dates)
                (into {})
                (redux/fuse))]
    (->> (transduce identity rf runs)
         (map (fn [[k v]] (assoc v :date k))))))

