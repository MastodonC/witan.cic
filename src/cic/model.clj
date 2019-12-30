(ns cic.model
  (:require [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.random :as rand]
            [cic.rscript :as rscript]
            [cic.spec :as spec]
            [cic.time :as time]
            [clj-time.core :as t]
            [clojure.math.combinatorics :as c]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]
            [kixi.stats.math :as m]
            [kixi.stats.protocols :as p]))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [{:keys [model-coefs gamma-params]}]
  (fn [age date seed]
    (let [{:keys [dispersion]} (get gamma-params age)
          shape (/ 1 dispersion)
          day (t/in-days (t/interval (t/epoch) date))
          intercept (:intercept model-coefs)
          a (get model-coefs (keyword (str "admission-age-" age)) 0.0)
          b (get model-coefs :beginning)
          c (get model-coefs (keyword (str "beginning:admission-age-" age)) 0.0)
          mean (m/exp (+ intercept a (* b day) (* c day)))]
      (p/sample-1 (d/gamma {:shape shape :scale (/ mean shape)}) seed))))

(defn joiners-model-gen
  "Wraps R to trend joiner rates into the future."
  [periods seed]
  (let [script "src/joiners.R"
        input (str (rscript/write-periods! periods))
        out1 (str (write/temp-file "file" ".csv"))
        out2 (str (write/temp-file "file" ".csv"))
        seed-long (rand/rand-long seed)]
    (rscript/exec script input out1 out2 (str (Math/abs seed-long)))
    (-> (read/joiner-csvs out1 out2)
        (joiners-model))))

(defn sample-ci
  "Given a 95% lower bound, median and 95% upper bound,
  sample from a skewed normal with these properties.
  We make use of the fact that the normal 95% CI is +/- 1.96"
  [lower median upper seed]
  (let [normal (p/sample-1 (d/normal {:mu 0 :sd 1}) seed)]
    (if (pos? normal)
      (+ median (* (- upper median) (/ normal 1.96)))
      (- median (* (- median lower) (/ normal -1.96))))))

(defn duration-model
  "Given an admitted date and age of a child in care,
  returns an expected duration in days"
  [coefs]
  (fn duration-model*
    ([birthday beginning seed]
     (duration-model* birthday beginning 0 seed))
    ([birthday beginning min-value seed]
     (let [age (time/year-interval birthday beginning)
           empirical (get coefs (max 0 (min age 17)))
           n (transduce (take-while (fn [[_ m _]] (<= m min-value))) k/count empirical)
           [r1 r2] (rand/split seed)
           max-value (dec (time/day-interval birthday (time/years-after birthday 18)))]
       (if (> n 100)
         (int (p/sample-1 (d/uniform {:a min-value :b max-value}) r1))
         (let [quantile (int (p/sample-1 (d/uniform {:a n :b 100}) r1))
               [lower median upper] (get empirical quantile)]
           (max min-value (min (sample-ci lower median upper r2) max-value))))))))

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
  (let [age-duration-lookup (reduce (fn [lookup {:keys [admission-age duration episodes period-id]}]
                                      (let [yrs (/ duration 365.0)]
                                        (update-fuzzy lookup [admission-age yrs] conj (map #(assoc % :period-id period-id) episodes))))
                                    {} closed-periods)
        age-duration-placement-offset-lookup (reduce (fn [lookup {:keys [admission-age duration episodes period-id]}]
                                                       (let [duration-yrs (/ duration 365.0)]
                                                         (reduce (fn [lookup {:keys [offset placement]}]
                                                                   (let [offset-yrs (/ offset 365)]
                                                                     (update-fuzzy lookup [admission-age duration-yrs placement offset-yrs] conj (map #(assoc % :period-id period-id) episodes))))
                                                                 lookup
                                                                 episodes)))
                                                     {} closed-periods)]
    (fn
      ([age duration seed]
       (let [duration-yrs (Math/round (/ duration 365.0))
             candidates (get age-duration-lookup [(min age 17) duration-yrs])
             candidate (rand/rand-nth candidates seed)]
         (if (seq candidate)
           (take-while #(< (:offset %) duration) candidate)
           [{:offset 0 :placement spec/unknown-placement}])))
      ([age duration {:keys [episodes] :as open-period} seed]
       (let [{:keys [placement offset]} (last episodes)]
         (let [duration-yrs (Math/round (/ duration 365.0))
               offset-yrs (Math/round (/ offset 365.0))
               candidates (get age-duration-placement-offset-lookup [(min age 17) duration-yrs placement offset-yrs])
               candidate (rand/rand-nth candidates seed)
               future-episodes (->> candidate
                                    (drop-while #(<= (:offset %) (:duration open-period)))
                                    (take-while #(< (:offset %) duration)))]
           (if (seq candidate)
             (concat episodes future-episodes)
             (let [last-offset (-> episodes last :offset)]
               (concat episodes [{:offset (inc last-offset)
                                  :placement spec/unknown-placement}])))))))))
