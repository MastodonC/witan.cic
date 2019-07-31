(ns cic.projection
  (:require [clojure.test.check.random :as r]
            [clojure.string :as str]
            [clj-time.core :as t]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]
            [redux.core :as redux]))

(defn interval-days
  [start stop]
  (t/in-days (t/interval start stop)))

(defn day-offset-in-year
  [date]
  (interval-days (t/date-time (t/year date)) date))

(defn prepare-ages
  "All we know about a child is their year of birth, so we impute an arbitrary birthday.
  Each projection will use randomly generated birthdays with corresponding random ages of admission.
  This allows the output to account for uncertainty in the input.
  The only constraint besides their year of birth is that a child can't be a negative age at admission"
  [open-periods seed]
  (let [rngs (r/split-n (r/make-random seed) (count open-periods))]
    (map (fn [{:keys [beginning dob] :as period} rng]
           (let [base-date (t/date-time dob 1 1)
                 max-offset (day-offset-in-year beginning)
                 offset (-> (if (= (-> period :beginning t/year) dob)
                              (d/uniform 0 max-offset)
                              (d/uniform 0 365))
                            (p/sample-1 rng))
                 birthday (t/plus base-date (t/days offset))]
             (-> period
                 (assoc :birthday birthday)
                 (assoc :admission-age (quot (interval-days birthday beginning) 365)))))
         open-periods rngs)))

(def interarrival-time
  (d/gamma {:shape 1.0 :scale 1.171895}))

(defn project-period-close
  "Sample a possible duration in care which is greater than the existing duration"
  [duration-model {:keys [duration beginning admission-age] :as open-period}]
  (let [projected-duration (loop [sample (duration-model admission-age)]
                             (if (>= sample duration)
                               (t/days sample)
                               (recur (duration-model admission-age))))]
    (-> (assoc open-period :duration projected-duration)
        (assoc :end (t/plus beginning projected-duration))
        (assoc :open? false))))

(defn joiners-seq
  [joiners-model beginning end]
  (let [wait-time (joiners-model beginning)
        next-time (t/plus beginning (t/days wait-time))]
    (when (t/before? next-time end)
      (let [period-end (t/plus next-time (t/days (d/draw lifetime)))
            period {:beginning next-time :end period-end}]
        (cons period (lazy-seq (joiners-seq joiners-model next-time end)))))))

(defn project-joiners
  [joiners-model beginning end]
  (mapcat (fn [age]
            (joiners-seq (partial joiners-model age) beginning end))
          (range 0 18)))

(defn day-seq
  "Create a sequence of dates with a 7-day interval between two dates"
  [beginning end]
  (when (t/before? beginning end)
    (lazy-seq
     (cons beginning
           (day-seq (t/plus beginning (t/days 7)) end)))))

(defn daily-summary
  "Takes inferred future periods and calculates the total CiC"
  [periods beginning end]
  (let [in-care? (fn [date]
                   (fn [{:keys [beginning end]}]
                     (and (t/before? beginning date)
                          (or (nil? end)
                              (t/after? end date)))))]
    (reduce (fn [output date]
              (assoc output date (transduce (filter (in-care? date)) k/count periods)))
            {} (day-seq beginning end))))

(defn project-1
  [open-periods beginning end joiners-model duration-model]
  (let [seed (rand-int 10000)]
    (-> (map (partial project-period-close duration-model) (prepare-ages open-periods 42))
        (concat (project-joiners joiners-model beginning end))
        (daily-summary beginning end))))

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
  [open-periods beginning end joiners-model duration-model n-runs]
  (->> (repeatedly n-runs #(project-1 open-periods beginning end joiners-model duration-model))
       (summarise)))
