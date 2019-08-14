(ns cic.projection
  (:require [cic.model :as model]
            [clojure.test.check.random :as r]
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
  [open-periods]
  (let [rngs (r/split-n (r/make-random) (count open-periods))]
    (map (fn [{:keys [beginning dob] :as period} rng]
           (let [base-date (t/date-time dob 1 1)
                 max-offset (day-offset-in-year beginning)
                 offset (-> (if (= (-> period :beginning t/year) dob)
                              (d/uniform {:a 0 :b max-offset})
                              (d/uniform {:a 0 :b 365}))
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
  [duration-model episodes-model {:keys [duration beginning admission-age episodes] :as open-period}]
  (let [projected-duration (loop [sample (duration-model admission-age)]
                             (if (>= sample duration)
                               sample
                               (recur (duration-model admission-age))))
        episodes (episodes-model admission-age projected-duration open-period)]
    (-> (assoc open-period :duration projected-duration)
        (assoc :episodes episodes)
        (assoc :end (t/with-time-at-start-of-day (t/plus beginning (t/days projected-duration))))
        (assoc :open? false))))

(defn joiners-seq
  [joiners-model duration-model episodes-model beginning end]
  (let [wait-time (joiners-model beginning)
        next-time (t/plus beginning (t/days wait-time))
        duration (duration-model)
        episodes (episodes-model duration)]
    (when (t/before? next-time end)
      (let [period-end (t/plus next-time (t/days duration))
            period {:beginning (t/with-time-at-start-of-day next-time)
                    :end (t/with-time-at-start-of-day period-end)
                    :episodes episodes}]
        (cons period (lazy-seq (joiners-seq joiners-model duration-model episodes-model next-time end)))))))

(defn project-joiners
  [joiners-model duration-model episodes-model beginning end]
  (mapcat (fn [age]
            (->> (joiners-seq (partial joiners-model age) (partial duration-model age) (partial episodes-model age) beginning end)
                 (map #(assoc % :birthday (t/minus (:beginning %) (t/years age))))))
          (range 0 18)))

(defn day-seq
  "Create a sequence of dates with a 7-day interval between two dates"
  [beginning end]
  (when (t/before? beginning end)
    (lazy-seq
     (cons beginning
           (day-seq (t/plus beginning (t/days 7)) end)))))

(defn episode-on
  [{:keys [beginning episodes]} date]
  (let [offset (t/in-days (t/interval beginning date))]
    ;; We want the last episode whose offset is less
    (some #(when (<= (:offset %) offset) %) (reverse episodes))))

(defn age-on
  [{:keys [birthday]} date]
  (t/in-years (t/interval birthday date)))

(defn daily-summary
  "Takes inferred future periods and calculates the total CiC"
  [periods beginning end]
  (let [in-care? (fn [date]
                   (fn [{:keys [beginning end]}]
                     (and (or (t/before? beginning date)
                              (t/equal? beginning date))
                          (or (nil? end)
                              (t/equal? end date)
                              (t/after? end date)))))
        placements [:Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1 :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0]
        placements-zero (zipmap placements (repeat 0))
        ages (range 0 19)
        ages-zero (zipmap ages (repeat 0))]
    (reduce (fn [output date]
              (let [in-care (filter (in-care? date) periods)
                    by-placement (->> (map #(:placement (episode-on % date)) in-care)
                                      (frequencies))
                    by-age (->> (map #(age-on % date) in-care)
                                (frequencies))]
                (assoc output date (merge-with + {:total (count in-care)}
                                               placements-zero by-placement
                                               ages-zero by-age))))
            {} (day-seq beginning end))))

(defn project-1
  [open-periods closed-periods beginning end joiners-model duration-model]
  (let [episodes-model (model/episodes-model (prepare-ages closed-periods))]
    (-> (map (partial project-period-close duration-model episodes-model) (prepare-ages open-periods))
        (concat (project-joiners joiners-model duration-model episodes-model beginning end))
        (daily-summary beginning end))))

(defn vals-histogram
  "Histogram reducing function for the the vals corresponding to `key`.
  Returns a summary of the histogram"
  [f]
  (-> k/histogram
      (redux/pre-step f)
      (redux/post-complete
       (fn [dist]
         {:lower (d/quantile dist 0.05)
          :q1 (d/quantile dist 0.25)
          :median (d/quantile dist 0.5)
          :q3 (d/quantile dist 0.75)
          :upper (d/quantile dist 0.95)}))))

(defn summarise
  "Creates a histogram reducing function over each key of the maps returned by the runs.
  Each histogram is summarised into a map with the date associated."
  [runs]
  (let [dates (->> runs first keys)
        placements [:Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1 :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0]
        placements-spec (fn [date]
                          (reduce (fn [rf-spec placement]
                                    (assoc rf-spec placement (redux/pre-step k/median #(get-in % [date placement]))))
                                  {} placements))
        ages (range 0 19)
        ages-spec (fn [date]
                    (reduce (fn [rf-spec age]
                              (assoc rf-spec age (redux/pre-step k/median #(get-in % [date age]))))
                            {} ages))
        rf-spec (reduce (fn [rf-spec date]
                          (assoc rf-spec date (redux/fuse (merge {:total (vals-histogram #(get-in % [date :total]))}
                                                                 (placements-spec date)
                                                                 (ages-spec date)))))
                        {}
                        dates)
        rf (redux/fuse rf-spec)]
    (->> (transduce identity rf runs)
         (map (fn [[k v]] (assoc v :date k))))))

(defn projection
  "Takes the open periods, creates n-runs projections and summarises them."
  [periods beginning end joiners-model duration-model n-runs]
  (let [open-periods (filter :open? periods)
        closed-periods (filter :end periods)]
   (->> (repeatedly n-runs #(project-1 open-periods closed-periods beginning end joiners-model duration-model))
        (summarise))))
