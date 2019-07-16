(ns cic.core
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.set :as cs]
            [clojure.string :as str]
            [tick.alpha.api :as t]))

(defn blank-row? [row]
  (every? str/blank? row))

(defn format-row
  [row]
  (-> (cs/rename-keys row {:id :child-id})
      (update :child-id #(Long/parseLong %))
      (update :dob #(Long/parseLong %))
      (update :report-date t/date)
      (update :ceased #(when-not (str/blank? %) (t/date %)))
      (update :report-year #(Long/parseLong %))
      (update :placement keyword)
      (update :care-status keyword)
      (update :legal-status keyword)
      (update :uasc (comp boolean #{"True"}))))

(defn load-csv
  "Loads csv file with each row as a vector.
   Stored in map separating column-names from data"
  ([filename]
   (let [parsed-csv (with-open [in-file (io/reader filename)]
                      (->> in-file
                           data-csv/read-csv
                           (remove blank-row?)
                           (vec)))
         parsed-data (rest parsed-csv)
         headers (map csk/->kebab-case-keyword (first parsed-csv))]
     (map (comp format-row (partial zipmap headers)) parsed-data))))

(defn remove-unmodelled-episodes [data]
  (remove (some-fn :uasc (comp #{:V4} :legal-status)) data))

(defn remove-stale-rows
  "The raw data may contain multiple open episodes for a child, one per report year that the episode was open.
  We want to remove open episodes from prior report years, since these are stale data.
  This function removes episodes without a cease date from report years prior to the latest."
  [data]
  (let [latest-report-year (->> data (map :report-year) (apply max))]
    (remove #(and (< (:report-year %) latest-report-year) (nil? (:ceased %))) data)))

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

(defn episodes
  "Takes a parsed CSV and returns cleansed episodes data"
  [csv]
  (->> csv
       (remove-stale-rows)
       (remove-unmodelled-episodes)))

(defn summarise-periods-at
  "Summarise a contiguous period of episodes at a point in time"
  [episodes timestamp]
  (let [first-episode (first episodes)
        last-episode (last episodes)]
    (-> (select-keys first-episode [:period-id :dob :report-date])
        (cs/rename-keys {:report-date :beginning})
        (assoc :open? (or (-> last-episode :ceased nil?)
                          (t/> (:ceased last-episode) timestamp)))
        (assoc :duration (t/days (t/duration (t/new-interval (:report-date first-episode) timestamp)))))))


(defn open-periods
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
         (map #(summarise-periods-at % projection-start))
         (filter :open?))))

(defn -main [filename]
  (let [data (load-csv filename)]
    (-> data
        (episodes)
        (open-periods))))
