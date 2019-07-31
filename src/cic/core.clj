(ns cic.core
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.set :as cs]
            [clojure.string :as str]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(defn blank-row? [row]
  (every? str/blank? row))

(def date-format
  (f/formatter :date))

(defn parse-date
  [s]
  (f/parse date-format s))

(defn parse-double
  [d]
  (Double/parseDouble d))

(defn parse-int
  [i]
  (int (Double/parseDouble i)))

(defn format-episode
  [row]
  (-> (cs/rename-keys row {:id :child-id})
      (update :child-id #(Long/parseLong %))
      (update :dob #(Long/parseLong %))
      (update :report-date parse-date)
      (update :ceased #(when-not (str/blank? %) (parse-date %)))
      (update :report-year #(Long/parseLong %))
      (update :placement keyword)
      (update :care-status keyword)
      (update :legal-status keyword)
      (update :uasc (comp boolean #{"True"}))))

(defn load-csv
  "Loads csv file with each row as a vector.
   Stored in map separating column-names from data"
  [filename]
  (let [parsed-csv (with-open [in-file (io/reader filename)]
                     (->> in-file
                          data-csv/read-csv
                          (remove blank-row?)
                          (vec)))
        parsed-data (rest parsed-csv)
        headers (map csk/->kebab-case-keyword (first parsed-csv))]
    (map (partial zipmap headers) parsed-data)))

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
                          (t/after? (:ceased last-episode) timestamp)))
        (assoc :duration (t/in-days (t/interval (:report-date first-episode) timestamp))))))

(defn summarise-period
  [episodes]
  (let [first-episode (first episodes)
        last-episode (last episodes)]
    (-> (select-keys first-episode [:period-id :dob :report-date])
        (cs/rename-keys {:report-date :beginning})
        (assoc :end (:ceased last-episode)))))

(defn episodes->periods
  [episodes]
  (->> (assoc-period-id episodes)
       (group-by :period-id)
       (vals)
       (map summarise-period)))

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

(defn csv->episodes
  [filename]
  (->> (load-csv filename)
       (map format-episode)
       (episodes)))

(defn load-duration-csv
  [filename]
  (let [parsed-csv (with-open [in-file (io/reader filename)]
                     (->> in-file
                          data-csv/read-csv
                          (remove blank-row?)
                          (vec)))
        parsed-data (rest parsed-csv)]
    (->> (map (comp (juxt first (comp vec rest)) (partial map parse-int)) parsed-data)
         (into {}))))

(defn load-duration-csvs
  [lower median upper]
  (let [lower (load-duration-csv lower)
        median (load-duration-csv median)
        upper (load-duration-csv upper)]
    (->> (for [age (range 0 18)]
           (let [lower (get lower age)
                 median (get median age)
                 upper (get upper age)]
             (vector age
                     (mapv (fn [l m u]
                             (vector l m u))
                           lower median upper))))
         (into {}))))

(defn load-age-csv
  [filename]
  (->> (load-csv filename)
       (map (juxt :coef (comp parse-double :value)))
       (into {})))
