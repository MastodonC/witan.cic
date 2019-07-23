(ns cic.repl
  (:require [clojure.string :as str]
            [cic.core :as core]
            [cic.projection :as projection]
            [clj-time.format :as f]))

(def date-format
  (f/formatter :date))

(defn date->str
  [date]
  (f/unparse date-format date))

(defn format-projection-tsv
  [projection]
  (let [fields (juxt (comp date->str :date) :actual :min :q1 :median :q3 :max)
        field-sep "\t"
        line-sep "\n"
        headers ["date" "actual" "min" "q1" "median" "q3" "max"]]
    (->> (for [future-estimate projection]
           (str/join field-sep (fields future-estimate)))
         (concat [(str/join field-sep headers)])
         (str/join line-sep))))

(defn project
  [episodes project-from project-to]
  (projection/projection (core/open-periods episodes)
                         project-from project-to 100))

(defn format-actual-for-output
  [[date count]]
  (hash-map :date date
            :actual count))

(defn episodes->projection-tsv
  [output-file episodes-file]
  (let [episodes (core/csv->episodes episodes-file)
        output-from (f/parse date-format "2010-03-31")
        project-from (f/parse date-format "2018-03-31")
        project-to (f/parse date-format "2025-03-31")
        summary (-> (core/csv->episodes episodes-file)
                    (core/episodes->periods)
                    (projection/daily-summary output-from project-from))
        summary-seq (map format-actual-for-output summary)
        projection (concat summary-seq
                           (project episodes project-from project-to))]
    (->> (format-projection-tsv projection)
         (spit output-file))))

#_(episodes->projection-tsv "output.tsv" "episodes.csv")