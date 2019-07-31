(ns cic.repl
  (:require [clojure.string :as str]
            [cic.core :as core]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [cic.model :as model]
            [cic.projection :as projection]
            [clj-time.format :as f]))

(def date-format
  (f/formatter :date))

(defn date->str
  [date]
  (f/unparse date-format date))

(defn write-projection-tsv
  [outfile projection]
  (let [fields (juxt (comp date->str :date) :actual :min :q1 :median :q3 :max)
        headers ["date" "actual" "min" "q1" "median" "q3" "max"]]
    (with-open [writer (io/writer outfile)]
      (data-csv/write-csv writer (concat [headers] (map fields projection))))))

(defn project
  [episodes project-from project-to joiners-model duration-model]
  (projection/projection (core/open-periods episodes)
                         project-from project-to
                         joiners-model duration-model
                         100))

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
        joiners-model (-> (core/load-age-csv "data/age-model.csv")
                          (model/joiners-model))
        duration-model (-> (core/load-duration-csvs "data/duration-model-lower.csv"
                                                    "data/duration-model-median.csv"
                                                    "data/duration-model-upper.csv")
                           (model/duration-model))
        summary (-> episodes
                    (core/episodes->periods)
                    (projection/daily-summary output-from project-from))
        summary-seq (map format-actual-for-output summary)
        projection (concat summary-seq
                           (project episodes project-from project-to
                                    joiners-model duration-model))]
    (write-projection-tsv "data/output.csv" projection)))

#_(episodes->projection-tsv "data/output.tsv" "data/episodes.csv")
