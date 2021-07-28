(ns cic.io.write
  (:require [cic.spec :as spec]
            [cic.time :as time]
            [cic.periods :as periods]
            [cic.time :as time]
            [clj-time.format :as f]
            [clojure.data.csv :as data-csv]
            [clojure.set :as set]
            [clojure.java.io :as io])
  (:import java.io.File))

(defn temp-file
  [prefix suffix]
  (doto (File/createTempFile prefix suffix)
    (.deleteOnExit)))

(def date-format
  (f/formatter :date))

(defn date->str
  [date]
  (f/unparse date-format date))

(defn projection-table
  [projection]
  (let [fields (apply juxt
                      (comp date->str :date)
                      :actual
                      :actual-cost
                      (comp :lower :projected)
                      (comp :q1 :projected)
                      (comp :median :projected)
                      (comp :q3 :projected)
                      (comp :upper :projected)
                      (comp :lower :projected-cost)
                      (comp :q1 :projected-cost)
                      (comp :median :projected-cost)
                      (comp :q3 :projected-cost)
                      (comp :upper :projected-cost)
                      (concat (map #(comp :median % :placements) spec/placements)
                              (map (fn [age] #(get-in % [:ages age :median])) spec/ages)))
        headers (concat ["Date" "Actual" "Cost"]
                        ["CiC Lower CI" "CiC Lower Quartile" "CiC Median" "CiC Upper Quartile" "CiC Upper CI"]
                        ["Cost Lower CI" "Cost Lower Quartile" "Cost Median" "Cost Upper Quartile" "Cost Upper CI"]
                        (map name spec/placements)
                        (map str spec/ages))]
    (into [headers]
          (map fields)
          projection)))

(defn period->episodes
  [t0 {:keys [period-id simulation-id beginning dob birthday admission-age episodes end provenance
              match-offset matched-id matched-offset] :as period}]
  (let [placement-sequence (transduce (comp (map (comp name :placement))
                                            (interpose "-"))
                                      str
                                      episodes)
        placement-pathway (transduce (comp (map (comp name :placement))
                                           (dedupe)
                                           (interpose "-"))
                                     str
                                     episodes)
        period-duration (time/day-interval beginning end)
        t (time/day-interval t0 beginning)]
    (into []
          (comp
           #_(filter (fn [[a b]]
                       (or (nil? b) (> (:offset b) (:offset a)))))
           (map-indexed (fn [idx [{:keys [placement offset]} to]]
                          (hash-map :period-id period-id
                                    :simulation-id simulation-id
                                    :episode-number (inc idx)
                                    :dob dob
                                    :admission-age admission-age
                                    :birthday birthday
                                    :start (time/days-after beginning offset)
                                    :end (or (some->> to :offset dec (time/days-after beginning)) end)
                                    :placement placement
                                    :offset offset
                                    :provenance provenance
                                    :placement-sequence placement-sequence
                                    :placement-pathway placement-pathway
                                    :period-start beginning
                                    :period-duration period-duration
                                    :period-end end
                                    :period-offset t
                                    :match-offset match-offset
                                    :matched-id matched-id
                                    :matched-offset matched-offset))))
          (partition-all 2 1 episodes))))

(defn episodes->table-rows-xf
  [project-to]
  (comp #_(filter (fn [{:keys [period-id dob episode start end placement]}]
                    (time/< start project-to)))
        (map (fn [{:keys [period-id simulation-id dob birthday admission-age
                          episode-number start end placement offset
                          provenance placement-sequence placement-pathway
                          period-start period-duration period-end period-offset
                          match-offset matched-id matched-offset] :as episode}]
               (vector simulation-id period-id
                       episode-number dob admission-age
                       (date->str birthday)
                       (date->str start)
                       (when end (date->str end)) ;; TODO - why would a period have no end date?
                       (name placement)
                       offset
                       provenance
                       placement-sequence
                       placement-pathway
                       (date->str period-start)
                       period-duration
  (mapv name [:simulation-id :period-from :period-to :age :n-per-day]))

(defn joiner-rates-tap
  [joiner-rates]
  (let [fields (juxt :simulation-id (comp date->str :period-from) (comp date->str :period-to) :age (comp double :n-per-day))]
    (into []
          (map fields)
          joiner-rates)))
