(ns cic.time
  (:refer-clojure :exclude [< <= = > >=])
  (:require [clj-time.core :as t]
            [clj-time.periodic :as p]
            [clj-time.format :as f]))

(def date-string (f/formatter "yyyy-MM-dd"))
(def month-string (f/formatter "yyyy-MM"))

(def < t/before?)

(def > t/after?)

(def = t/equal?)

(def earliest t/earliest)

(def latest t/latest)

(defn <=
  [a b]
  (or (< a b)
      (= a b)))

(defn >=
  [a b]
  (or (> a b)
      (= a b)))

(defn between?
  [x a b]
  (and (>= x a)
       (< x b)))

(defn years-after
  [date yrs]
  (t/plus date (t/years yrs)))

(defn days-after
  [date days]
  (t/plus date (t/days days)))

(defn years-before
  [date yrs]
  (t/minus date (t/years yrs)))

(defn days-before
  [date days]
  (t/minus date (t/days days)))

(defn day-seq
  "Create a sequence of dates with an n-day interval between two dates"
  ([beginning end]
   (day-seq beginning end 1))
  ([beginning end days]
   (p/periodic-seq beginning end (t/days days))))

(defn month-seq
  ([beginning end]
   (month-seq beginning end 1))
  ([beginning end months]
   (p/periodic-seq beginning end (t/months months))))

(defn day-interval
  [from to]
  (t/in-days (t/interval from to)))

(defn year-interval
  [from to]
  (t/in-years (t/interval from to)))

(defn day-offset-in-year
  [date]
  (day-interval (t/date-time (t/year date)) date))

(defn max-date
  [dates]
  (apply t/max-date dates))

(defn min-date
  [dates]
  (apply t/min-date dates))

(def make-date t/date-time)

(def year t/year)

(def without-time t/with-time-at-start-of-day)

(defn month-beginning
  [date]
  (t/date-time (t/year date) (t/month date)))

(defn month-end
  [date]
  (-> (month-beginning date)
      (t/plus (t/months 1))
      (t/minus (t/days 1))))

(defn month-as-string
  [date]
  (f/unparse month-string date))

(defn date-as-string
  [date]
  (f/unparse date-string date))

(defn quarter-following
  [date]
  (let [test (t/minus date (t/days 1))
        ;; Map months to quarters
        ;; For all days in January except the first, the following quarter starts on 1st April
        ;; For all days in April except the first, the following quarter starts on 1st July
        ;; etc
        quarter (inc (mod (+ (* (quot (dec (t/month test)) 3) 3) 3) 12))
        ;; For any dates where the next quarter is January, increment the year
        year (if (== quarter 1) (inc (t/year test)) (t/year test))]
    (t/date-time year quarter)))

(defn quarter-preceding
  [date]
  (t/minus (quarter-following (t/plus date (t/days 1))) (t/months 3)))

(defn financial-year-end
  [date]
  (let [year-end (t/date-time (t/year date) 3 31)]
    (if (<= date year-end)
      year-end
      (years-after year-end 1))))

(defn financial-year-seq
  "Given a seed date, return a lazy seq of subsequent year-ends"
  [date]
  (p/periodic-seq (financial-year-end date)
                  (t/years 1)))

(defn day-before-18th-birthday
  [birthday]
  (t/minus (t/plus birthday (t/years 18)) (t/days 1)))

(defn halfway-between
  [a b]
  (let [d (quot (day-interval a b) 2)]
    (days-after a d)))
