(ns cic.project
  (:require [clojure.test.check.random :as r]
            [clojure.string :as str]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]
            [redux.core :as redux]
            [tick.alpha.api :as t]
            [tick.core :as tick]))

(defn day-offset-in-year
  [date]
  (t/days (t/duration (t/new-interval (t/year date) date))))

(defn interval-duration
  [start stop]
  (t/duration (t/new-interval start stop)))

(defn prepare-ages
  "All we know about a child is their year of birth, so we impute an arbitrary birthday.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input.
  The only constraint besides their year of birth is that a child can't be a negative age at admission"
  [open-periods seed]
  (let [rngs (r/split-n (r/make-random seed) (count open-periods))]
    (map (fn [{:keys [beginning dob] :as period} rng]
           (let [base-date (t/new-date dob 1 1)
                 max-offset (day-offset-in-year beginning)
                 offset (-> (if (= (-> period :beginning t/year) (t/year dob))
                              (d/uniform 0 max-offset)
                              (d/uniform 0 365))
                            (p/sample-1 rng))
                 birthday (tick/forward-number base-date offset)]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (quot (t/days (interval-duration birthday beginning)) 365)))))
         open-periods rngs)))

(def lifetime
  "FIXME: the distribution paramaters will depend on admission age, placement..."
  (d/weibull {:shape 0.7131747 :scale 1020.9653067}))

(defn lifetime-gt
  "Sample a value > threshold from the distribution"
  [threshold]
  (loop [sample (int (d/draw lifetime))]
    (if (> sample threshold)
      sample
      (recur (int (d/draw lifetime))))))

(defn project-period-close
  "Sample a possible duration in care which is greater than the existing duration"
  [{:keys [duration beginning] :as open-period}]
  (let [projected-duration (lifetime-gt duration)]
    (-> (assoc open-period :duration projected-duration)
        (assoc :end (tick/forward-number beginning projected-duration))
        (assoc :open? false))))

(defn day-seq
  "Create a sequence of dates with a 7-day interval between two dates"
  [beginning end]
  (when (t/<= beginning end)
    (lazy-seq
     (cons beginning
           (day-seq (tick/forward-number beginning 7) end)))))

(defn daily-summary
  "Takes inferred future periods and calculates the total CiC"
  [periods beginning end]
  (-> (reduce
       (fn [[output candidates] date]
         (let [candidates (filter (fn [{:keys [end]}]
                                    (t/>= end date))
                                  candidates)]
           (vector (assoc output date (count candidates)) candidates)))
       [{} periods]
       (day-seq beginning end))
      (first)))

(defn project-1
  [periods beginning end]
  (-> (map project-period-close open-periods)
      (daily-summary beginning end)))

(defn vals-histogram
  "Histogram reducing function for the the vals corresponding to `key`.
  Returns a summary of the histogram"
  [key]
  (-> k/histogram
      (redux/pre-step #(get % key))
      (redux/post-complete d/summary)))

(defn summarise
  "Creates a histogram reducing function over each key of the maps returned by the runs.
  Each histogram is summarised into a map with the date associated."
  [runs]
  (let [dates (->> runs first keys)
        rf-spec (reduce (fn [rf-spec date]
                          (assoc rf-spec date (vals-histogram date)))
                        {}
                        dates)
        rf (redux/fuse rf-spec)]
    (->> (transduce identity rf runs)
         (map (fn [[k v]] (assoc v :date k))))))

(defn projection
  "Takes the open periods, creates n-runs projections and summarises them."
  [open-periods beginning end n-runs]
  (->> (repeatedly n-runs #(project-1 open-periods beginning end))
       (summarise)))

(defn format-projection
  [projection]
  (let [fields (juxt (comp str :date) :min :q1 :median :q3 :max)
        field-sep "\t"
        line-sep "\n"]
    (->> (for [future-estimate projection]
           (str/join field-sep (fields future-estimate)))
         (str/join line-sep))))

;; (format-projection (project (map project-period-close (prepare-ages open-periods 42)) (t/date "2018-03-31") (t/date "2025-03-31") 1000))
