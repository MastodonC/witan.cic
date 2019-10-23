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
            [clojure.set :as cs]))

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
  []
  (hash-map :periods (-> (read/episodes "data/episodes.csv")
                         (episodes/scrub-episodes)
                         (periods/from-episodes))
            :placement-costs (read/costs-csv "data/placement-costs.csv")
            :duration-model (-> (read/duration-csvs "data/duration-model-lower.csv"
                                                    "data/duration-model-median.csv"
                                                    "data/duration-model-upper.csv")
                                (model/duration-model))))

(defn format-actual-for-output
  [[date summary]]
  (-> (assoc summary :date date)
      (cs/rename-keys {:count :actual
                       :cost :actual-cost})))

(defn write-projection!
  "Main REPL function for writing a projection CSV"
  [output-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs)
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
        summary-seq (->> (summary/periods-summary (rand/prepare-ages periods (rand/seed seed))
                                                  (time/day-seq output-from project-from 7)
                                                  placement-costs)
                         (map format-actual-for-output))
        projection (projection/projection projection-seed
                                          model-seed
                                          (time/day-seq project-from project-to 7)
                                          placement-costs
                                          seed n-runs)]
    (write/projection-output! output-file (concat summary-seq projection))))

(defn write-validation!
  "Outputs model projection and linear regression projection together with actuals for comparison."
  [out-file n-runs seed]
  (let [{:keys [periods placement-costs duration-model]} (load-model-inputs)]
    (->> (for [as-at (time/month-seq (time/make-date 2010 1 1)
                                     (time/make-date 2010 3 1))
               :let [periods-as-at (periods/periods-as-at periods as-at)
                     learn-from (time/years-before as-at 2)
                     project-to (time/years-after as-at 1)
                     projection-seed {:seed (filter :open? periods-as-at)
                                      :date as-at}
                     model-seed {:seed periods
                                 :duration-model duration-model
                                 :joiner-range [learn-from as-at]}]]
           (hash-map :date as-at
                     :model (validate/model projection-seed model-seed project-to {:seed seed :n-runs n-runs})
                     :comparison (validate/linear-regression periods-as-at project-to)
                     :actual (validate/actual periods project-to)))
         (write/validation-output! out-file))))

(defn write-episodes!
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
         (write/episodes-output! out-file project-to))))
