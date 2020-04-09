(ns cic.periods
  (:require [cic.time :as time]
            [clojure.set :as cs]
            [taoensso.timbre :as timbre]))

(defn period-id
  "Period ID is a composite key of the child's ID and a period number"
  [episode period-index]
  (format "%s-%s" (:child-id episode) period-index))

(defn assoc-period-id
  "Each child has a unique ID. Some children leave care and return again later.
  Since each period in care is modelled separately, we assign each period its own ID."
  [data]
  (let [timelines (sort-by (juxt :child-id :report-date) data)
        period-ids (reductions (fn [period [one next]]
                                 (cond (not= (:child-id one) (:child-id next)) 0
                                       (not= (:ceased one) (:report-date next)) (inc period)
                                       :else period))
                               0
                               (partition 2 1 timelines))]
    (map #(assoc %1 :period-id (period-id %1 %2)) timelines period-ids)))

(defn summarise-periods
  "Summarise a contiguous period of episodes at a point in time"
  [episodes]
  (let [first-episode (first episodes)
        beginning (:report-date first-episode)
        last-episode (last episodes)]
    (-> (select-keys first-episode [:period-id :birth-month :report-date])
        (cs/rename-keys {:report-date :beginning})
        (assoc :end (:ceased last-episode))
        (assoc :episodes (mapv (fn [{:keys [placement report-date]}]
                                 (hash-map :placement placement
                                           :offset (time/day-interval beginning report-date)))
                               episodes)))))

(defn assoc-open-at
  [{:keys [beginning end] :as period} date]
  (let [open? (and (time/<= beginning date)
                   (or (nil? end)
                       (time/> end date)))]
    (-> (assoc period :open? open?)
        (assoc :duration (time/day-interval beginning (or end date))))))

(defn from-episodes
  "Takes episodes data and returns just the open periods ready for projection"
  [episodes]
  (let [projection-start (->> (mapcat (juxt :report-date :ceased) episodes)
                              (remove nil?)
                              (sort)
                              (last))]
    (->> episodes
         (assoc-period-id)
         (group-by :period-id)
         (vals)
         (map (comp #(assoc-open-at % projection-start)
                    summarise-periods)))))

(defn periods-as-at
  "This function takes the periods that have occurred and creates a view of how they would have appeared at some point in the past"
  [periods as-at]
  (->> (filter #(time/<= (:beginning %) as-at) periods)
       (map (fn [{:keys [episodes beginning end open? duration] :as period}]
              (if (or open? (time/>= end as-at))
                (let [duration (time/day-interval beginning as-at)]
                  (-> period
                      (assoc :open? true)
                      (dissoc :end)
                      (assoc :duration duration)
                      (update :episodes (fn [episodes] (vec (filter #(<= (:offset %) duration) episodes))))))
                period)))))

(defn episode-on
  [{:keys [beginning episodes]} date]
  (let [offset (time/day-interval beginning date)]
    ;; We want the last episode whose offset is less
    (some #(when (<= (:offset %) offset) %) (reverse episodes))))

(defn age-on
  "Return the age of a child on the given date"
  [{:keys [birthday]} date]
  (time/year-interval  birthday date))

(defn in-care?
  [date]
  (fn [{:keys [beginning end]}]
    (and (time/<= beginning date)
         (or (nil? end)
             (time/>= end date)))))

(defn assoc-birthday-bounds
  [periods]
  (into []
        (comp (map (fn [{:keys [beginning reported birth-month end period-id] :as period}]
                     (let [ ;; Earliest possible birthday is either January 1st in the year of their birth
                           ;; or 18 years prior to their final end date (or current report date if not yet ended),
                           ;; whichever is the later
                           earliest-birthday (time/latest (time/years-before (or end reported) 18)
                                                          (time/month-beginning birth-month))
                           ;; Latest possible birthday is either December 31st in the year of their birth
                           ;; or the date they were taken into care, whichever is the sooner
                           latest-birthday (time/earliest beginning
                                                          (time/month-end birth-month))]
                       (if (time/>= latest-birthday earliest-birthday)
                         (assoc period :birthday-bounds [earliest-birthday latest-birthday])
                         (do (timbre/info (format "Birthday for %s can't be inferred, removing" period-id))
                             nil)))))
              (keep identity))
        periods))
