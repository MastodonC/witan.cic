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
  (fn []
    (let [model (rand-nth ages)]
      (fn [age date]
        (let [{:keys [dispersion]} (get params age)
              shape (/ 1 dispersion)
              day (t/in-days (t/interval (t/epoch) date))

              intercept (:intercept model)
              a (get model (keyword (str "age-" age)) 0.0)
              b (get model :beginning)
              c (get model (keyword (str "beginning:age-" age)) 0.0)
              mean (m/exp (+ intercept a (* b day) (* c day)))]
          (d/draw (d/gamma {:shape shape :scale (/ mean shape)})))))))

(defn sample-ci
  "Given a 95% lower bound, median and 95% upper bound,
  sample from a skewed normal with these properties.
  We make use of the fact that the normal 95% CI is +/- 1.96"
  [lower median upper]
  (let [normal (d/draw (d/normal {:mu 0 :sd 1}))]
    (if (pos? normal)
      (+ median (* (- upper median) (/ normal 1.96)))
      (- median (* (- median lower) (/ normal -1.96))))))

(defn duration-model
  "Given an admitted date and age of a child in care,
  returns an expected duration in days"
  [coefs]
  (fn [age]
    (let [empirical (get coefs (max 0 (min age 17)))
          quantile (inc (rand-int 100))
          [lower median upper] (get empirical quantile)]
      (sample-ci lower median upper))))

(defn update-fuzzy
  "Like `update`, but the key is expected to be a vector of values.
  Any numeric values in the key are fuzzed, so for example
  `(update-fuzzy {} [5 0.5] conj :value)` will return:
  {(4 0) (:value),
   (4 1) (:value),
   (5 0) (:value),
   (5 1) (:value),
   (6 0) (:value),
   (6 1) (:value)}
  This enables efficient lookup of values which are similar to,
  but not neccessarily identical to, the input key."
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
      ([age duration {:keys [episodes] :as open-period}]
       (let [{:keys [placement offset]} (last episodes)]
         (let [duration-yrs (Math/round (/ duration 365.0))
               offset-yrs (Math/round (/ offset 365.0))
               candidates (get age-duration-placement-offset-lookup [(min age 17) duration-yrs placement offset-yrs])
               candidate (rand-nth candidates)
               future-episodes (->> candidate
                                    (drop-while #(<= (:offset %) (:duration open-period)))
                                    (take-while #(< (:offset %) duration)))]
           (concat episodes future-episodes)))))))
