(ns cic.repl
  (:require [clojure.string :as str]
            [clojure.set :as cs]
            [clojure.test.check.random :as r]
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
  (let [fields (juxt (comp date->str :date)
                     :cost
                     (comp :actual)
                     (comp :lower :total)
                     (comp :q1 :total)
                     (comp :median :total)
                     (comp :q3 :total)
                     (comp :upper :total)
                     :Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1 :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0
                     #(get % 0) #(get % 1) #(get % 2) #(get % 3) #(get % 4) #(get % 5) #(get % 6) #(get % 7) #(get % 8) #(get % 9)
                     #(get % 10) #(get % 11) #(get % 12) #(get % 13) #(get % 14) #(get % 15) #(get % 16) #(get % 17) #(get % 18))
        placements [:Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1 :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0]
        ages (range 0 19)
        headers (concat ["Date" "Cost" "Actual" "Lower CI" "Lower Quartile" "Median" "Upper Quartile" "Upper CI"]
                        (map name placements)
                        (map str ages))]
    (with-open [writer (io/writer outfile)]
      (data-csv/write-csv writer (concat [headers] (map fields projection))))))

(defn project
  [episodes project-from project-to joiners-model duration-model seed]
  (let [periods (core/episodes->periods episodes)]
    (projection/projection periods
                           project-from project-to
                           joiners-model duration-model
                           seed
                           1)))

(defn format-actual-for-output
  [[date summary]]
  (-> (assoc summary :date date)
      (cs/rename-keys {:total :actual})))

(defn assoc-costs
  [placement-costs placement-counts]
  (let [cost (reduce (fn [total {:keys [placement cost]}]
                       (+ total (* cost (get placement-counts placement 0))))
                     0 placement-costs)]
    (assoc placement-counts :cost cost)))

(defn episodes->projection-tsv
  [output-file episodes-file seed]
  (let [episodes (core/csv->episodes episodes-file)
        output-from (f/parse date-format "2015-03-31")
        project-from (f/parse date-format "2018-03-31")
        project-to (f/parse date-format "2025-03-31")
        placement-costs (core/load-costs-csv "data/placement-costs.csv")
        joiners-model (-> (core/load-joiner-csvs "data/joiner-model-mvn.csv"
                                                 "data/joiner-model-params.csv")
                          (model/joiners-model))
        duration-model (-> (core/load-duration-csvs "data/duration-model-lower.csv"
                                                    "data/duration-model-median.csv"
                                                    "data/duration-model-upper.csv")
                           (model/duration-model))
        summary (-> episodes
                    (core/episodes->periods)
                    (projection/prepare-ages (r/make-random seed))
                    (projection/daily-summary output-from project-from))
        summary-seq (map format-actual-for-output summary)
        projection (->> (concat summary-seq
                                (project episodes project-from project-to
                                         joiners-model duration-model seed))
                        (map (partial assoc-costs placement-costs)))]
    (write-projection-tsv output-file projection)))

#_(episodes->projection-tsv "data/witan.cic.output.ci.csv" "data/episodes.csv" 42)
