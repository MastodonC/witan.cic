(ns cic.core
  (:require [camel-snake-kebab.core :as csk]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.string :refer [blank?]]
            [tick.alpha.api :as t]))

(defn blank-row? [row]
  (every? blank? row))

(defn format-row
  [row]
  (-> row
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

(defn -main [filename]
  (let [data (load-csv filename)]
    (-> data
        (remove-stale-items)
        (remove-unmodelled-episodes))))
