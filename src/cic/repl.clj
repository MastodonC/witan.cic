(ns cic.repl
  (:require [clojure.core.async :as a]
            [cic.episodes :as episodes]
            [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.model :as model]
            [cic.periods :as periods]
            [cic.projection :as projection]
            [cic.random :as rand]
            [cic.spec :as spec]
            [cic.summary :as summary]
            [cic.time :as time]
            [cic.validate :as validate]
            [clojure.set :as cs]
            [net.cgrand.xforms :as xf]
            [net.cgrand.xforms.rfs :as xrf]
            [kixi.stats.core :as k]
            [net.cgrand.xforms :as x]))

(set! *warn-on-reflection* true)

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
  ([{:keys [episodes-csv placement-costs-csv duration-lower-csv duration-median-csv duration-upper-csv]}]
   (hash-map :periods (-> (read/episodes episodes-csv)
                          (periods/from-episodes))
             :placement-costs (when placement-costs-csv (read/costs-csv placement-costs-csv))
             :duration-model (-> (read/duration-csvs duration-lower-csv
                                                     duration-median-csv
                                                     duration-upper-csv)
                                 (model/duration-model))))
  ([]
   (load-model-inputs {:episodes-csv "data/episodes.scrubbed.csv"
                       :placement-costs-csv "data/placement-costs.csv"
                       :duration-lower-csv "data/duration-model-lower.csv"
                       :duration-median-csv "data/duration-model-median.csv"
                       :duration-upper-csv "data/duration-model-upper.csv"})))

(defn prepare-model-inputs
  [{:keys [periods] :as model-inputs}]
  (let [report-date (->> (mapcat (juxt :beginning :end) periods)
                         (keep identity)
                         (time/max-date))
        periods (map #(assoc % :reported report-date) periods)]
    (assoc model-inputs :periods periods)))

(defn format-actual-for-output
  [[date summary]]
  (-> (assoc summary :date date)
      (cs/rename-keys {:count :actual
                       :cost :actual-cost})))

(defn generate-projection-csv!
  "Main REPL function for writing a projection CSV"
  [output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (prepare-model-inputs (load-model-inputs))
        project-from (time/max-date (map :beginning periods))
        project-to (time/years-after project-from 3)
        learn-from (time/years-before project-from 4)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        output-from (time/years-before learn-from 2)
        summary-seq (into []
                          (map format-actual-for-output)
                          (summary/periods-summary (rand/prepare-ages periods (rand/seed seed))
                                                   (time/day-seq output-from project-from 7)
                                                   placement-costs))
        projection (projection/projection projection-seed
                                          model-seed
                                          (time/day-seq project-from project-to 7)
                                          placement-costs
                                          seed n-runs)]
    (->> (write/projection-table (concat summary-seq projection))
         (write/write-csv! output-file))))

(defn generate-annual-csv!
  [output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs)
        project-from (time/days-after (time/financial-year-end (time/max-date (map :beginning periods))) 1)
        project-to (time/financial-year-end (time/years-after project-from 5))
        learn-from (time/years-before project-from 10)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        costs-lookup (into {} (map (juxt :placement :cost) placement-costs))
        actuals-by-year-age (into {} (comp (filter #(time/< (:beginning %) project-from))
                                           (xf/by-key (juxt (comp time/year time/financial-year-end :beginning) :admission-age)
                                                      (xf/reduce k/count)))
                                  (rand/prepare-ages periods (rand/seed seed)))
        actuals-by-year (into {} (xf/by-key ffirst second (xf/reduce +)) actuals-by-year-age)
        actuals (->> (reduce (fn [coll [year joiners]]
                               (-> (assoc-in coll [year :actual-joiners] joiners)
                                   (assoc-in [year :year] year)))
                             (reduce (fn [coll [[year age] joiners]]
                                       (assoc-in coll [year :joiners-ages age] joiners))
                                     {} actuals-by-year-age)
                             actuals-by-year)
                     (vals))
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        output-from (time/years-before learn-from 2)
        _(println project-from project-to learn-from output-from)
        cost-projection (-> (into []
                                  (filter #(< (time/year project-from)
                                              (:year %)
                                              (time/year project-to)))
                                  (projection/cost-projection projection-seed
                                                              model-seed
                                                              project-to
                                                              placement-costs
                                                              seed n-runs))
                            (into actuals))]
    (->> (write/annual-report-table cost-projection)
         (write/write-csv! output-file))))

(defn period->placement-seq
  "Takes a period and returns the sequence of placements as AA-BB-CC.
  Consecutive episodes in the same placement are collapsed into one."
  [{:keys [episodes] :as period}]
  (transduce
   (comp (map :placement)
         (partition-by identity)
         (map (comp name first))
         (interpose "-"))
   xrf/str
   episodes))

(defn generate-placement-sequence-csv!
  [output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs)
        project-from (time/max-date (map :beginning periods))
        project-to (time/financial-year-end (time/years-after project-from 3))
        learn-from (time/years-before project-from 4)
        closed-periods (remove :open? periods)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        output-from (time/years-before learn-from 2)]
    (let [freduce (partial xf/into [] (xf/by-key (juxt :admission-age period->placement-seq) xf/count))
          age-summary (partial xf/into {} (xf/by-key ffirst second (xf/reduce +)))
          age-sequence-totals (->> (rand/split-n (rand/seed seed) n-runs)
                                   (pmap (fn [seed]
                                           (freduce (projection/project-1 projection-seed model-seed project-to seed))))
                                   (apply concat)
                                   (into {} (xf/by-key (xf/reduce +))))
          actual-age-sequence-totals (freduce (rand/prepare-ages closed-periods (rand/seed seed)))
          age-totals (age-summary age-sequence-totals)]
      (->> {:projected-age-sequence-totals age-sequence-totals
            :projected-age-totals age-totals
            :actual-age-sequence-totals actual-age-sequence-totals
            :actual-age-totals (age-summary actual-age-sequence-totals)}
           (write/placement-sequence-table)
           (write/write-csv! output-file)))))

(defn generate-validation-csv!
  "Outputs model projection and linear regression projection together with actuals for comparison."
  [out-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs)
        validation (into []
                         (map #(validate/compare-models-at % duration-model periods seed n-runs))
                         (time/month-seq (time/make-date 2010 1 1)
                                         (time/make-date 2010 3 1)))]
    (->> (write/validation-table validation)
         (write/write-csv! out-file))))

(defn generate-episodes-csv!
  "Outputs a file showing a single projection in rowise episodes format."
  [out-file seed]
  (let [{:keys [periods duration-model]} (load-model-inputs)
        project-from (time/max-date (map :beginning periods))
        project-to (time/years-after project-from 3)
        learn-from (time/years-before project-from 4)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-range [learn-from project-from]}]
    (->> (projection/project-1 projection-seed model-seed project-to (rand/seed seed))
         (write/episodes-table project-to)
         (write/write-csv! out-file))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; core.async prototype


(comment
  ;; core.async flow
  (def results
    (time
     (let [ ;; core.async setup
           input-chan (a/chan 32)
           input-mult (a/mult input-chan)

           ;; simulation setup
           seed 42
           input-root "/home/bld/wip/cic-data/test-data"
           model-input-locations {:episodes-csv (str input-root "/episodes.scrubbed.csv")
                                  :duration-lower-csv (str input-root "/duration-model-lower.csv")
                                  :duration-median-csv (str input-root "/duration-model-median.csv")
                                  :duration-upper-csv (str input-root "/duration-model-upper.csv")
                                  :placement-costs-csv (str input-root "/placement-costs.csv")}
           {:keys [periods placement-costs duration-model]
            :as inputs}
           (-> model-input-locations load-model-inputs prepare-model-inputs)
           project-from (time/max-date (map :beginning periods))
           project-to (time/years-after project-from 3)
           project-dates (time/day-seq project-from project-to 7)
           n-runs 10
           learn-from (time/years-before project-from 4)
           projection-seed {:seed (filter :open? periods)
                            :date project-from}
           model-seed {:seed periods
                       :duration-model duration-model
                       :joiner-range [learn-from project-from]}
           max-date (time/max-date project-dates)
           ;; n-projections (into [] (projection/project-n projection-seed model-seed project-dates seed n-runs))

           ;; reducing taps
           single-projection (a/into [] (a/take 1 (a/tap input-mult (a/chan (a/dropping-buffer 1)))))
           all-projections (a/into [] (a/tap input-mult (a/chan 32)))

           ;; placements
           placement-summary-mult
           (a/mult
            (a/tap input-mult (a/chan 512 (map #(summary/placements-summary % project-dates)))))
           placement-summary-mapped-results (a/into [] (a/tap placement-summary-mult (a/chan 32)))
           placement-weekly-summary (a/transduce
                                     (mapcat identity)
                                     (summary/summarize-timeseries-rf spec/placements)
                                     (sorted-map)
                                     (a/tap placement-summary-mult (a/chan 512)))

           ;; ages
           ages-summary-mult
           (a/mult
            (a/tap input-mult (a/chan 512 (map #(summary/ages-summary % project-dates)))))
           ages-summary-mapped-results (a/into [] (a/tap ages-summary-mult (a/chan 32)))
           ages-weekly-summary (a/transduce
                                (mapcat identity)
                                (summary/summarize-timeseries-rf spec/ages)
                                (sorted-map)
                                (a/tap ages-summary-mult (a/chan 512)))

           ;; bed nights per month
           bed-nights-chan (a/chan 512)
           bed-nights-mult
           (a/mult bed-nights-chan)
           #_(a/mult
              (a/tap input-mult (a/chan 512 (map summary/bed-nights-per-month))))
           _ (a/mult
              (a/pipeline 3
                          bed-nights-chan
                          (map summary/bed-nights-per-month)
                          (a/tap input-mult (a/chan 512))))
           bed-nights-mapped-results (a/into [] (a/tap bed-nights-mult (a/chan 32)))
           bed-nights-per-month-summary (a/transduce
                                         (mapcat identity)
                                         (summary/summarize-timeseries-rf spec/placements)
                                         (sorted-map)
                                         (a/tap bed-nights-mult (a/chan 512)))
           ]

       ;; put the projections on the channel
       ;; (a/pipe (a/to-chan n-projections) input-chan)
       ;; (a/pipe (a/to-chan (projection/project-n projection-seed model-seed project-dates seed n-runs)) input-chan)
       (a/pipeline 3
                   input-chan
                   (map #(projection/project-1 projection-seed model-seed max-date %))
                   (a/to-chan (-> (rand/seed seed)
                                  (rand/split-n n-runs))))
       {:single-projection (a/<!! single-projection)
        :all-projections (a/<!! all-projections)
        :placement-weekly-summary (a/<!! placement-weekly-summary)
        :placement-summary-mapped-results (a/<!! placement-summary-mapped-results)
        :ages-summary-mapped-results (a/<!! ages-summary-mapped-results)
        :ages-weekly-summary (a/<!! ages-weekly-summary)
        :bed-nights-mapped-results (a/<!! bed-nights-mapped-results)
        :bed-nights-per-month-summary (a/<!! bed-nights-per-month-summary)
        :project-from project-from
        :project-to project-to
        :project-dates project-dates
        })))


  spec/placements
  [:Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1 :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0 :NA]

  (def bed-nights-report
    (into [(into ["date"] (map name spec/placements))]
          (comp
           (map (fn [[date placements]]
                  (into [(clj-time.format/unparse (clj-time.format/formatter "YYYY-MM") date)]
                        (map (fn [p]
                               (get-in placements [p :median])))
                        spec/placements)))
           (x/sort-by first))
          (:bed-nights-per-month-summary results)))

  (require '[dk.ative.docjure.spreadsheet :as xl])

  (let [wb (xl/create-workbook "Bed Nights Per Month" bed-nights-report)]
    (xl/save-workbook! "bed-nights-report.xls" wb))

  )
