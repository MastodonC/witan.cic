(ns cic.model
  (:require [clj-time.core :as t]
            [clojure.math.combinatorics :as c]
            [kixi.stats.math :as m]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [{:keys [ages params]}]
  (fn [age date]
    (let [{:keys [shape rate]} (get params age)
          day (t/in-days (t/interval (t/epoch) date))
          intercept (get ages "(Intercept)")
          a (get ages (str "age" age) 0)
          b (get ages "beginning")
          c (get ages (str "beginning:age" age) 0)
          mean (m/exp (+ intercept a (* b day) (* c day)))]
      (d/draw (d/gamma {:shape shape :scale (/ mean shape)})))))

(defn duration-model
  "Given an admitted date and age of a child in care,
  returns an expected duration in days"
  [coefs]
  (fn [age]
    (let [empirical (get coefs (max 0 (min age 17)))
          quantile (inc (rand-int 100))
          [lower median upper] (get empirical quantile)
          normal (d/draw (d/normal {:mu 0 :sd 1}))]
      (if (pos? normal)
        (+ median (* (- upper median) (/ normal 1.96)))
        (- median (* (- median lower) (/ normal -1.96)))))))

(defn update-fuzzy
  [coll ks f & args]
  (let [ks (mapv (fn [k]
                   (cond
                     (or (double? k) (ratio? k))
                     [(int (m/floor k)) (int (m/ceil k))]
                     (int? k)
                     [(dec k) k (inc k)]
                     :else [k]))
                 ks)]
    (reduce (fn [coll ks]
              (apply update coll ks f args))
            coll
            (apply c/cartesian-product ks))))

(defn episodes-model
  "Given an age of admission and duration,
  sample likely placements from input data"
  [closed-periods]
  (let [age-duration-lookup (reduce (fn [lookup {:keys [admission-age duration episodes]}]
                                      (let [yrs (/ duration 365.0)]
                                        (update-fuzzy lookup [admission-age yrs] conj episodes)))
                                    {} closed-periods)
        age-duration-placement-offset-lookup (reduce (fn [lookup {:keys [admission-age duration episodes]}]
                                                       (let [duration-yrs (/ duration 365.0)]
                                                         (reduce (fn [lookup {:keys [offset placement]}]
                                                                   (let [offset-yrs (/ offset 365)]
                                                                     (update-fuzzy lookup [admission-age duration-yrs placement offset-yrs] conj episodes)))
                                                                 lookup
                                                                 episodes)))
                                                     {} closed-periods)]
    (fn
      ([age duration]
       (let [duration-yrs (Math/round (/ duration 365.0))
             candidates (get age-duration-lookup [(min age 17) duration-yrs])]
         (rand-nth candidates)))
      ([age duration episodes]
       (let [{:keys [placement offset]} (last episodes)]
         (let [duration-yrs (Math/round (/ duration 365.0))
               offset-yrs (Math/round (/ offset 365.0))
               candidates (get age-duration-placement-offset-lookup [(min age 17) duration-yrs placement offset-yrs])
               candidate (rand-nth candidates)
               future-episodes (->> candidate
                                    (drop-while #(<= (:offset %) offset))
                                    (take-while #(< (:offset %) duration)))]
           (concat episodes future-episodes)))))))
