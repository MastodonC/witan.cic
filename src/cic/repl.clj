(ns cic.repl
  (:require [cic.episodes :as episodes]
            [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.model :as model]
            [cic.periods :as periods]
            [cic.projection :as projection]
            [cic.random :as rand]
            [cic.summary :as summary]
            [cic.time :as time]
            [cic.validate :as validate]
            [clojure.set :as cs]
            [net.cgrand.xforms :as xf]
            [net.cgrand.xforms.rfs :as xrf]
            [redux.core :as rx]
            [kixi.stats.core :as k]
            [clojure.core.async :as a]
            [redux.core :as redux]))

(set! *warn-on-reflection* true)

(defn debug-log [m]
  (prn m))

(add-tap debug-log)

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
<<<<<<< HEAD
  []
  (hash-map :periods (-> (read/episodes "data/episodes.scrubbed.csv")
                         (periods/from-episodes))
            :placement-costs (read/costs-csv placement-costs-csv)
=======
  [{:keys [episodes-csv placement-costs-csv duration-lower-csv duration-median-csv duration-upper-csv]}]
  (hash-map :periods (into []
                           (-> (read/episodes episodes-csv)
                               (episodes/scrub-episodes)
                               (periods/from-episodes)))
            :placement-costs (into [] (read/costs-csv placement-costs-csv))
>>>>>>> WIP
            :duration-model (-> (read/duration-csvs duration-lower-csv
                                                    duration-median-csv
                                                    duration-upper-csv)
                                (model/duration-model))))

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


(defn project-n-run []
  (tap> {:debug "project-n-run started"})
  ;; setup let
  (let [n-runs 10
        seed 42
        model-input-locations {:episodes-csv "/home/bld/wip/cic/witan.csc.suffolk/data/episodes.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/episodes.csv"
                               :duration-lower-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-lower.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-lower.csv"
                               :duration-median-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-median.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-median.csv"
                               :duration-upper-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-upper.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-upper.csv"
                               ;; only Suffolk costs
                               :placement-costs-csv "/home/bld/wip/cic/witan.csc.suffolk/data/placement-costs.csv"}
        {:keys [periods placement-costs duration-model]} (load-model-inputs model-input-locations)
        project-from (time/days-after (time/financial-year-end (time/max-date (map :beginning periods))) 1)
        project-to (time/financial-year-end (time/years-after project-from 2))
        learn-from (time/years-before project-from 10)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        project-dates (time/day-seq project-from project-to 7)
        simulation-runs (into [] (projection/project-n projection-seed model-seed project-dates seed n-runs))]

    (tap> {:debug "Simulation runs generated"})
    simulation-runs
    ;; core.async let
    #_(let [input-chan (a/chan 512)
            input-mult (a/mult input-chan)
            period-summary-chan (a/tap input-mult (a/chan 512 (map (fn [sim-run]
                                                                     (tap> {:debug "Running periods-summary"})
                                                                     (summary/periods-summary sim-run project-dates placement-costs)))))
            period-summary-mult (a/mult period-summary-chan)
            projected-population (a/transduce (map (fn [[k v]] [k (:count v)]))
                                              (fn
                                                ([a] (into {} (map (fn [[k v]] [k (summary/histogram-rf v)])) a))
                                                ([acc [k v]] (if-let [histogram (:k acc)]
                                                               (assoc acc k (summary/histogram-rf histogram v))
                                                               (assoc acc k (-> (summary/histogram-rf)
                                                                                (summary/histogram-rf v))))))
                                              {}
                                              (a/tap period-summary-mult (a/chan 512 (mapcat identity))))
            sample-period-summary (a/into [] (a/tap period-summary-mult (a/chan 32)))]
        (tap> {:debug "core.async machinery set up"})
        (a/pipe (a/to-chan simulation-runs) input-chan)
        (tap> {:debug "Data pushed to pipe"})
        {:projected-population (a/<!! projected-population)
         :sample-period-summary (a/<!! sample-period-summary)})))

(comment

  (def project-n-run-results (project-n-run))


  (def foo
    (let [n-runs 2
          seed 42
          model-input-locations {:episodes-csv "/home/bld/wip/cic/witan.csc.suffolk/data/episodes.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/episodes.csv"
                                 :duration-lower-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-lower.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-lower.csv"
                                 :duration-median-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-median.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-median.csv"
                                 :duration-upper-csv "/home/bld/wip/cic/witan.csc.suffolk/data/duration-model-upper.csv" #_"/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-upper.csv"
                                 ;; only Suffolk costs
                                 :placement-costs-csv "/home/bld/wip/cic/witan.csc.suffolk/data/placement-costs.csv"}
          {:keys [periods placement-costs duration-model]} (load-model-inputs model-input-locations)
          project-from (time/days-after (time/financial-year-end (time/max-date (map :beginning periods))) 1)
          project-to (time/financial-year-end (time/years-after project-from 2))
          project-dates (time/day-seq project-from project-to 7)
          ;; simulation-runs (into [] (projection/project-n projection-seed model-seed project-dates seed n-runs))
          ]
      {:periods periods
       :placement-costs placement-costs
       :project-dates project-dates}
      ))

  (first (:project-dates foo))

  (require '[cic.periods :as periods])
  (def periods-by-date
    (into {}
          (map (fn [date]
                 [date (filter (periods/in-care? date) (:periods foo))]))
          (count (:project-dates foo))))


  )

(comment

  ;; TODO: call projection/project-n by tweaking load-model-inputs and see what result we can push onto a channel and mults

  ;; Norfolk Config
  (def norfolk-model-inputs
    {:episodes-csv "/home/bld/wip/cic/witan.csc.norfolk/data/episodes.csv"
     :duration-lower-csv "/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-lower.csv"
     :duration-median-csv "/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-median.csv"
     :duration-upper-csv "/home/bld/wip/cic/witan.csc.norfolk/data/duration-model-upper.csv"
     ;; only Suffolk costs
     :placement-costs-csv "/home/bld/wip/cic/witan.csc.suffolk/data/placement-costs.csv"})

  (generate-projection-csv! norfolk-model-inputs "foo.csv" 2 42)

  ;; TODO: This works now that I have all the R code installed. What
  ;; are the next things to do with this data? Do I want to treat it
  ;; all as one big seq or do I want to keep it as a seq of seqs?
  ;;
  ;; summary/combo-rf is the fused reduction step that needs to be
  ;; driving the data products it is called from summary/grand-summary
  (def project-n-run-results (project-n-run))

  (require '[cic.summary :as summary])
  (def periods-summary (summary/periods-summary (first project-n-run-results)))
  )

(defn generate-annual-csv!
  [model-input-locations output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs model-input-locations)
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
  [model-input-locations output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs model-input-locations)
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
  [model-input-locations out-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs model-input-locations)
        validation (into []
                         (map #(validate/compare-models-at % duration-model periods seed n-runs))
                         (time/month-seq (time/make-date 2010 1 1)
                                         (time/make-date 2010 3 1)))]
    (->> (write/validation-table validation)
         (write/write-csv! out-file))))

(defn generate-episodes-csv!
  "Outputs a file showing a single projection in rowise episodes format."
  [model-input-locations out-file seed]
  (let [{:keys [periods duration-model]} (load-model-inputs model-input-locations)
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

(comment

  (defn debug-log [m]
    (prn m))

  (add-tap debug-log)
  (remove-tap debug-log)

  )
