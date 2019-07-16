(ns cic.core
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.set :refer [rename-keys]]
            [clojure.string :refer [blank?]]
            [tick.alpha.api :as t]))

(defn blank-row? [row]
  (every? blank? row))

(defn format-row
  [row]
  (-> (rename-keys row {:id :child-id})
      (update :child-id #(Long/parseLong %))
      (update :dob #(Long/parseLong %))
      (update :report-date t/date)
      (update :ceased #(when-not (blank? %) (t/date %)))
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

(defn -main [filename]
  (let [data (load-csv filename)]
    (-> data
        (remove-stale-rows)
        (remove-unmodelled-episodes)
        (assoc-period-id))))
