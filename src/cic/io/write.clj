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
                       (date->str period-end)
                       period-offset
                       match-offset
                       matched-id
                       matched-offset)))))

(defn episodes-table
  [t0 project-to projections]
  (let [headers ["Simulation" "ID" "Episode" "Birth Year" "Admission Age" "Birthday" "Start" "End" "Placement" "Offset" "Provenance"
                 "Placement Sequence" "Placement Pathway" "Period Start" "Period Duration" "Period End" "Period Offset"
                 "Match Offset" "Matched ID" "Matched Offset"]]
    (into [headers]
          (comp cat
                (mapcat (partial period->episodes t0))
                (episodes->table-rows-xf project-to))
          projections)))

(defn validation-table
  [validation]
  (let [headers ["Type" "Metric" "Actual" "Projected" "Lower Quartile" "Upper Quartile"]
        fields (juxt (comp date->str :date) :model :linear-regression :actual)]
    (->> (for [[type comparison] validation
               [value {:keys [actual projected q1 q3]}] comparison]
           (vector (name type) value actual projected q1 q3))
         (into [headers]))))

(defn annual-report-table
  [cost-projection]
  (let [headers (concat ["Financial Year End" "Joiners Actual"]
                        ["Cost Lower CI" "Cost Lower Quartile" "Cost Median" "Cost Upper Quartile" "Cost Upper CI"]
                        ["Joiners Lower CI" "Joiners Lower Quartile" "Joiners Median" "Joiners Upper Quartile" "Joiners Upper CI"]
                        (map name spec/placements)
                        (map str spec/ages))
        fields (apply juxt
                      :year
                      :actual-joiners
                      (comp :lower :projected-cost)
                      (comp :q1 :projected-cost)
                      (comp :median :projected-cost)
                      (comp :q3 :projected-cost)
                      (comp :upper :projected-cost)
                      (comp :lower :projected-joiners)
                      (comp :q1 :projected-joiners)
                      (comp :median :projected-joiners)
                      (comp :q3 :projected-joiners)
                      (comp :upper :projected-joiners)
                      (concat (map #(comp % :placements) spec/placements)
                              (map (fn [age] #(get-in % [:joiners-ages age])) spec/ages)))]
    (into [headers]
          (map fields)
          cost-projection)))

(defn duration-table
  [periods]
  (let [headers (concat ["Admission Age" "Duration" "Provenance"])
        fields (juxt :admission-age :duration :provenance)]
    (into [headers]
          (map fields)
          periods)))

(defn placement-sequence-table
  [{:keys [projected-age-sequence-totals projected-age-totals
           actual-age-sequence-totals actual-age-totals]}]
  (let [headers ["Actual / Projected" "Age" "Placement Sequence" "Proportion"]]
    (-> (into [headers]
              (map (fn [[[age sequence] count]]
                     (vector "Projected" age sequence (double (/ count (get projected-age-totals age))))))
              projected-age-sequence-totals)
        (into (map (fn [[[age sequence] count]]
                     (vector "Actual" age sequence (double (/ count (get actual-age-totals age))))))
              actual-age-sequence-totals))))

(defn write-csv!
  [out-file tablular-data]
  (with-open [writer (io/writer out-file)]
    (data-csv/write-csv writer tablular-data)))

(defn mapseq->csv!
  [mapseq]
  (let [path (temp-file "file" ".csv")
        cols (-> mapseq first keys)]
    (->> (into [(mapv name cols)]
               (map (apply juxt cols))
               mapseq)
         (write-csv! path))
    path))

(defn periods->knn-closed-cases-csv
  [periods]
  (->> (periods/to-mapseq periods)
       (map #(-> %
                 (update :placement name)
                 (update :beginning time/date-as-string)
                 (update :end (fn [end] (when end (time/date-as-string end))))
                 (update :birthday time/date-as-string)
                 (update :snapshot-date time/date-as-string)
                 (update :report-date time/date-as-string)
                 (set/rename-keys {:report-date :report_date :snapshot-date :snapshot_date :period-id :period_id})))
       (mapseq->csv!)
       (str)))

(defn segments-table
  [segments]
  (let [headers (concat ["Age" "Duration" "Date" "From Placement" "To Placement" "Age Days" "Offset" "Aged Out" "Care Days" "Initial" "Terminal" "Join Age Days"])
        fields (juxt :age :duration (comp date->str :date) :from-placement :to-placement :age-days
                     :offset :aged-out :care-days :initial? :terminal? :join-age-days)]
    (into [headers]
          (map fields)
          segments)))

(defn periods-universe
  [periods]
  (let [headers ["Provenance" "ID" "Sample Index" "Admission Age" "Admission Age Days" "Duration" "Episodes EDN" "Aged Out"]
        fields (juxt :provenance :period-id :iteration :admission-age :admission-age-days :duration (comp pr-str :episodes) (comp boolean :aged-out?))]
    (into [headers]
          (map fields)
          periods)))

(def joiner-rate-headers
  (mapv name [:simulation-id :date :date :n-per-month :n-per-day]))

(defn joiner-rates
  [joiner-rates]
  (let [fields (juxt :simulation-id (comp date->str :day) :age
                     (comp double :n-per-month)
                     (comp double :n-per-day))]
    (into []
          (map fields)
          joiner-rates)))

(def joiner-rates-headers
  (mapv name [:simulation-id :period-from :period-to :age :n-per-day
              :n-per-period :y-per-period]))

(defn joiner-rates-tap
  [joiner-rates]
  (let [fields (juxt :simulation-id (comp date->str :period-from) (comp date->str :period-to) :age
                     (comp double :n-per-day)
                     (comp double :n-per-period)
                     (comp double :y-per-period))]
    (into []
          (map fields)
          joiner-rates)))

(def joiner-scenario-headers
  (mapv name [:simulation-id :date :age :n-per-day :n-per-month]))

(defn joiner-scenario-tap
  [joiner-rates]
  (let [fields (juxt :simulation-id (comp date->str :day) :age
                     (comp double :n-per-day)
                     (comp double :n-per-month))]
    (into []
          (map fields)
          joiner-rates)))

(def joiner-interval-headers
  (mapv name [:simulation-id :age :n-per-day :join-date :target-rate :interval-days :interval-adjustment]))

(defn joiner-interval-tap
  [joiner-intervals]
  (let [fields (juxt :simulation-id :age :n-per-day
                     (comp date->str :join-date)
                     (comp double :target-rate)
                     (comp double :interval-days)
                     :interval-adjustment)]
    (into []
          (map fields)
          joiner-intervals)))
