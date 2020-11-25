(ns cic.periods
  (:require [cic.time :as time]
            [cic.episodes :as episodes]
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
                                 (cond (not= (:child-id one) (:child-id next)) 1
                                       (not= (:ceased one) (:report-date next)) (inc period)
                                       :else period))
                               1
                               (partition 2 1 timelines))]
    (map #(assoc %1 :period-id (period-id %1 %2)) timelines period-ids)))

(defn summarise-periods
  "Summarise a contiguous period of episodes at a point in time"
  [episodes]
  (let [first-episode (first episodes)
        beginning (:report-date first-episode)
        last-episode (last episodes)]
    (-> (select-keys first-episode [:period-id :birth-month :report-date :cluster])
        (cs/rename-keys {:report-date :beginning})
        (assoc :end (:ceased last-episode))
        (assoc :episodes (->> (mapv (fn [{:keys [placement report-date]}]
                                      (hash-map :placement placement
                                                :offset (time/day-interval beginning report-date)))
                                    episodes)
                              episodes/simplify)))))

(defn assoc-open-at
  [{:keys [beginning end] :as period} date]
  (let [open? (and (time/<= beginning date)
                   (or (nil? end)
                       (time/> end date)))]
    (-> (assoc period :open? open?)
        (assoc :duration (time/day-interval beginning (or end date))))))

(defn positive-duration?
  [{:keys [report-date ceased] :as episode}]
  (time/<= report-date (or ceased report-date)))

(defn from-episodes
  "Takes episodes data and returns just the open periods ready for projection"
  [episodes]
  (let [projection-start (->> (mapcat (juxt :report-date :ceased) episodes)
                              (remove nil?)
                              (sort)
                              (last))]
    (->> episodes
         (filter positive-duration?)
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
                      (assoc :snapshot-date as-at)
                      (update :episodes (fn [episodes] (vec (filter #(<= (:offset %) duration) episodes))))))
                (assoc period :snapshot-date as-at))))))

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
  "Impute birthday bounds for each child based on their month of birth and episodes.
  Constraints:
  - Month of birth must match the provided month
  - A child can't be a negative age at admission
  - A child must have left by the time they are 18
  If these constraints can't be satisfied, a message is logged and the child is excluded."
  [periods]
  (into []
        (comp (map (fn [{:keys [beginning snapshot-date birth-month end period-id] :as period}]
                     (let [ ;; Earliest possible birthday is either the 1st day in the month of their birth
                           ;; or 18 years prior to their final end date (or current report date if not yet ended),
                           ;; whichever is the later.
                           earliest-birthday (time/latest (time/years-before (or end snapshot-date) 18)
                                                          (time/month-beginning birth-month))
                           ;; Latest possible birthday is either the last day of the month of their birth
                           ;; or the date they were taken into care, whichever is the earlier
                           latest-birthday (time/earliest beginning
                                                          (time/month-end birth-month))]
                       (if (time/>= latest-birthday earliest-birthday)
                         (assoc period :birthday-bounds [earliest-birthday latest-birthday])
                         (do (timbre/info (format "Birthday for %s can't be inferred, removing" period-id))
                             nil)))))
              (keep identity))
        periods))

(defn to-mapseq
  [periods]
  (mapcat (fn [{:keys [beginning end period-id open? episodes birthday snapshot-date]}]
            (map (fn [{:keys [offset placement]}]
                   {:period-id period-id
                    :beginning beginning
                    :birthday birthday
                    :end end
                    :open open?
                    :placement placement
                    :report-date (time/days-after beginning offset)
                    :snapshot-date snapshot-date})
                 episodes))
          periods))

(defn segment
  [{:keys [beginning end birthday duration episodes open?]} id-offset]
  (let [segment-interval 365
        max-date (time/years-after birthday 18)]
    (map
     (fn [segment-time idx]
       (let [reversed-episodes (reverse episodes)
             segment-duration (min (- duration segment-time) segment-interval)
             from-age (time/year-interval birthday (time/days-after beginning segment-time))
             [prior-episodes segment-episodes] (->> episodes
                                                    (split-with (fn [{:keys [offset placement]}]
                                                                  (<= offset segment-time))))
             prior-episode (some-> (last prior-episodes)
                                   (assoc :offset 0))
             segment-episodes (concat (when prior-episode [prior-episode])
                                      (->>  (take-while (fn [{:keys [offset placement]}]
                                                          (<= offset (+ segment-time segment-interval)))
                                                        segment-episodes)
                                            (map (fn [placement]
                                                   (update placement :offset - segment-time)))))
             from-placement (->> segment-episodes first :placement)
             to-placement (->> segment-episodes last :placement)
             terminal? (< segment-duration segment-interval)]
         {:id (+ idx id-offset)
          :date (time/days-after beginning segment-time)
          :from-placement from-placement ;; starting placement
          :to-placement to-placement
          :age from-age ;; in years?
          :prior-care-days segment-time
          :terminal? terminal?
          :duration segment-duration ;; duration may not be full segment if they leave
          :episodes (episodes/simplify segment-episodes)
          :aged-out? (and terminal? ;; Only set for terminal segment
                          end
                          (or (time/>= end max-date)
                              (<= (time/day-interval end max-date) 50)))}))
     (range 0 duration segment-interval)
     (map inc (range)))))

(defn head-segment
  "Takes a segment and the number of days to take from the beginning"
  [{:keys [duration episodes] :as segment} to]
  (let [episodes (->> episodes
                      (take-while #(< (:offset %) to)))]
    (-> segment
        (assoc :duration to)
        (assoc :episodes episodes))))

(defn tail-segment
  "Takes a segment and the number of days to trim off the beginning"
  [{:keys [duration] :as segment} by]
  (when (< by duration)
    (let [episodes (mapv #(update % :offset - by) (:episodes segment))
          episodes (->> episodes
                        (drop (->> episodes (filter #(<= (:offset %) 0)) count dec))
                        (mapv #(update % :offset max 0)))]
      (-> segment
          (update :duration - by)
          (assoc :from-placement (-> episodes first :placement))
          (assoc :episodes episodes)))))



