(ns cic.time
  (:refer-clojure :exclude [< <= = > >=])
  (:require [clj-time.core :as t]
            [clj-time.periodic :as p]))

(def < t/before?)

(def > t/after?)

(def = t/equal?)

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
  "Create a sequence of dates with a 7-day interval between two dates"
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

(def make-date t/date-time)

(def year t/year)

(def without-time t/with-time-at-start-of-day)
