(ns cic.io.write
  (:require [cic.spec :as spec]
            [cic.time :as time]
            [clj-time.format :as f]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io])
  (:import java.io.File))

(defn temp-file
  [prefix suffix]
  (doto (File/createTempFile prefix suffix)
    (.deleteOnExit)))

(defn write-mapseq
  [mapseq]
  (let [path (temp-file "file" ".csv")
        cols (-> mapseq first keys)]
    (with-open [writer (io/writer path)]
      (data-csv/write-csv writer
                          (concat (vector (map name cols))
                                  (map (apply juxt cols) mapseq))))
    path))

(def date-format
  (f/formatter :date))

(defn date->str
  [date]
  (f/unparse date-format date))

(defn projection-output!
  [out-file projection]
  (let [fields (apply juxt
                      (comp date->str :date)
                      :cost
                      :actual
                      (comp :lower :total)
                      (comp :q1 :total)
                      (comp :median :total)
                      (comp :q3 :total)
                      (comp :upper :total)
                      (concat (map #(comp % :placements) spec/placements)
                              (map (fn [age] #(get-in % [:ages age])) spec/ages)))
        headers (concat ["Date" "Cost" "Actual" "Lower CI" "Lower Quartile" "Median" "Upper Quartile" "Upper CI"]
                        (map name spec/placements)
                        (map str spec/ages))]
    (with-open [writer (io/writer out-file)]
      (data-csv/write-csv writer (concat [headers] (map fields projection))))))

(defn episodes-output!
  [out-file project-to projection]
  (with-open [writer (io/writer out-file)]
    (->> projection
         (mapcat (fn [{:keys [period-id beginning duration dob episodes end]}]
                   (->> (partition-all 2 1 episodes)
                        (filter (fn [[a b]]
                                  (or (nil? b) (> (:offset b) (:offset a)))))
                        (map-indexed (fn [idx [{:keys [placement offset]} to]]
                                       (hash-map :period-id period-id
                                                 :dob dob
                                                 :episode (inc idx)
                                                 :start (time/days-after beginning offset)
                                                 :end (or (some->> to :offset dec (time/days-after beginning)) end)
                                                 :placement placement)))
                        (filter (fn [{:keys [period-id dob episode start end placement]}]
                                  (time/< start project-to)))
                        (map (fn [{:keys [period-id dob episode start end placement] :as period}]
                               (vector period-id dob episode
                                       (date->str start)
                                       (when (time/< end project-to) (date->str end))
                                       (name placement)))))))
         (concat [(vector "ID" "DOB" "Episode" "Start" "End" "Placement")])
         (data-csv/write-csv writer))))

(defn validation-output!
  [out-file validation]
  (with-open [writer (io/writer out-file)]
    (->> (concat
          (vector ["Date" "Model" "Comparison" "Actual"])
          (map (juxt (comp date->str :date) :model :comparison :actual) validation))
         (data-csv/write-csv writer))))

