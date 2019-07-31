(ns cic.model
  (:require [clj-time.core :as t]
            [kixi.stats.math :as m]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [coefs]
  (fn [age date]
    (let [day (t/in-days (t/interval (t/epoch) date))
          intercept (get coefs "(Intercept)")
          a (get coefs (str "age" age) 0)
          b (get coefs "beginning")
          c (get coefs (str "beginning:age" age) 0)
          scale (m/exp (+ intercept a (* b day) (* c day)))]
      (d/draw (d/gamma {:shape 1.0 :scale scale})))))

(defn duration-model
  "Given an admitted date and age of a child in care,
  returns an expected duration in days"
  [coefs]
  (fn [age]
    (let [empirical (get coefs age)
          quantile (inc (rand-int 100))
          [lower median upper] (get empirical quantile)
          normal (d/draw (d/normal {:mu 0 :sd 1}))]
      (if (pos? normal)
        (+ median (* (- upper median) (/ normal 1.96)))
        (- median (* (- median lower) (/ normal -1.96)))))))
