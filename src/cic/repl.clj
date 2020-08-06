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
            [kixi.stats.core :as k]))

  (def ccc "data/ccc/2020-06-09/%s")
  (def ncc "data/ncc/2020-06-09/%s")
  (def scc "data/scc/2020-07-16/%s")

(def input-format
  scc)

(def input-file (partial format input-format))
(def output-file (partial format input-format))

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
  ([{:keys [episodes-csv placement-costs-csv duration-lower-csv duration-median-csv duration-upper-csv
            zero-joiner-day-ages-csv survival-hazard-csv knn-closed-cases-csv]}]
   (let [episodes (read/episodes episodes-csv)
         project-from (->> (mapcat (juxt :report-date :ceased) episodes)
                           (keep identity)
                           (time/max-date))]
     (hash-map :project-from project-from
               :periods (periods/from-episodes episodes)
               :placement-costs (read/costs-csv placement-costs-csv)
               :knn-closed-cases (read/knn-closed-cases knn-closed-cases-csv)
               :joiner-birthday-model (-> (read/zero-joiner-day-ages zero-joiner-day-ages-csv)
                                          (model/joiner-birthday-model)))))
  ([]
   (load-model-inputs {:episodes-csv (format input-format "episodes.scrubbed.clustered.csv")
                       :placement-costs-csv (input-file "placement-costs.csv")
                       :duration-lower-csv (input-file "duration-model-lower.csv")
                       :duration-median-csv (input-file "duration-model-median.csv")
                       :duration-upper-csv (input-file "duration-model-upper.csv")
                       :zero-joiner-day-ages-csv (input-file "zero-joiner-day-ages.csv")
                       :survival-hazard-csv (input-file "survival-hazard.csv")
                       :knn-closed-cases-csv (input-file "knn-closed-cases.csv")})))

(defn prepare-model-inputs
  [{:keys [project-from periods] :as model-inputs}]
  (let [periods (->> (map #(assoc % :reported project-from) periods)
                     (periods/assoc-birthday-bounds))]
    (assoc model-inputs
           :periods periods)))

(defn format-actual-for-output
  [[date summary]]
  (-> (assoc summary :date date)
      (cs/rename-keys {:count :actual
                       :cost :actual-cost})
      (update :placements #(into {} (map (fn [[k v]] (vector k {:median v}))) %))
      (update :ages #(into {} (map (fn [[k v]] (vector k {:median v}))) %))
      (update :placement-ages #(into {} (map (fn [[k v]] (vector k {:median v}))) %))))

(defn generate-projection-csv!
  "Main REPL function for writing a projection CSV"
  [rewind-years train-years project-years n-runs seed]
  (let [output-file (output-file (format "projection-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s-cease-model-out.csv" rewind-years train-years project-years n-runs seed))
        {:keys [project-from periods placement-costs duration-model joiner-birthday-model knn-closed-cases]} (prepare-model-inputs (load-model-inputs))
        ;; project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        project-from (time/years-before project-from rewind-years)
        _ (println (str "Project from " project-from))
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        projection-periods (periods/periods-as-at periods project-from)
        projection-seed {:seed (filter :open? projection-periods)
                         :date project-from}
        model-seed {:seed projection-periods
                    :duration-model duration-model
                    :knn-closed-cases knn-closed-cases
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]
                    :project-to project-to}
        output-from (time/years-before learn-from 2)
        summary-seq (into []
                          (map format-actual-for-output)
                          (summary/periods-summary (rand/sample-birthdays projection-periods (rand/seed seed))
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
  [output-file rewind-years train-years project-years n-runs seed]
  (let [{:keys [periods placement-costs duration-model joiner-birthday-model]} (load-model-inputs)
        project-from (time/days-after (time/financial-year-end (time/max-date (map :beginning periods))) rewind-years)
        project-to (time/financial-year-end (time/years-after project-from project-years))
        learn-from (time/years-before project-from train-years)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        costs-lookup (into {} (map (juxt :placement :cost) placement-costs))
        actuals-by-year-age (into {} (comp (filter #(time/< (:beginning %) project-from))
                                           (xf/by-key (juxt (comp time/year time/financial-year-end :beginning) :admission-age)
                                                      (xf/reduce k/count)))
                                  (rand/sample-birthdays periods (rand/seed seed)))
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
                    :joiner-birthday-model joiner-birthday-model
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
  [output-file train-years project-years n-runs seed]
  (let [{:keys [periods placement-costs duration-model joiner-birthday-model]} (load-model-inputs)
        project-from (time/max-date (map :beginning periods))
        project-to (time/financial-year-end (time/years-after project-from project-years))
        learn-from (time/years-before project-from train-years)
        closed-periods (remove :open? periods)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
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
          actual-age-sequence-totals (freduce (rand/sample-birthdays closed-periods (rand/seed seed)))
          age-totals (age-summary age-sequence-totals)]
      (->> {:projected-age-sequence-totals age-sequence-totals
            :projected-age-totals age-totals
            :actual-age-sequence-totals actual-age-sequence-totals
            :actual-age-totals (age-summary actual-age-sequence-totals)}
           (write/placement-sequence-table)
           (write/write-csv! output-file)))))

(defn generate-validation-csv!
  "Outputs model projection and linear regression projection together with actuals for comparison."
  [train-years n-runs seed]
  (let [rewind-years 1
        project-years 1
        output-file (output-file (format "validation-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s.csv" rewind-years train-years project-years n-runs seed))
        {:keys [project-from periods placement-costs duration-model joiner-birthday-model]} (prepare-model-inputs (load-model-inputs))
        project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        _ (println (str "Project from " project-from))
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        projection-periods (periods/periods-as-at periods project-from)
        projection-seed {:seed (filter :open? projection-periods)
                         :date project-from}
        model-seed {:seed projection-periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]
                    :project-to project-to}
        projection (->> (projection/project-n projection-seed model-seed [project-to] seed n-runs)
                        (map #(summary/periods-summary % [project-to] placement-costs))
                        (summary/grand-summary)
                        (first))
        actuals (-> (summary/periods-summary (rand/sample-birthdays periods (rand/seed seed))
                                             [project-to]
                                             placement-costs)
                    (first)
                    (format-actual-for-output))]
    (->> (validate/compare-projected projection actuals)
         (write/validation-table)
         (write/write-csv! output-file))))

(defn generate-episodes-csv!
  "Outputs a file showing a single projection in rowise episodes format."
  [rewind-years train-years project-years n-runs seed]
  (let [output-file (output-file (format "episodes-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s-cease-model-out.csv" rewind-years train-years project-years n-runs seed))
        _ (println output-file)
        {:keys [project-from periods placement-costs duration-model joiner-birthday-model] :as model-inputs} (prepare-model-inputs (load-model-inputs))
        project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        _ (println (str "Project from " project-from))
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        periods (periods/periods-as-at periods project-from)
        projection-seed {:seed periods
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}]
    (->> (projection/project-n projection-seed model-seed [project-to] seed n-runs)
         (write/episodes-table project-to)
         (write/write-csv! output-file))))
