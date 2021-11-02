(ns cic.cic
  (:require [cic.repl :as repl]
            [cic.rscript :as rscript]
            [cic.time :as time]))

(defn run-cic-workflow
  [config]
  (repl/generate-projection-csv! config))

(defn run-generate-candidates-workflow
  [config]
  (repl/generate-candidates! config))

(defn run-rejection-sampling
  [{{:keys [episodes age-out-proportions
            generated-candidates target-distribution
            candidates-simulation candidates-projection
            candidates-age-out-projection candidates-age-out-simulation]} :file-inputs
    output-directory :output-directory
    {:keys [random-seed episodes-extract-date]} :projection-parameters}]
  (let [script "resources/R/rejection-sampling.R"]
    (rscript/exec script
                  episodes generated-candidates
                  (time/date-as-string episodes-extract-date)
                  target-distribution age-out-proportions
                  candidates-simulation candidates-projection
                  candidates-age-out-simulation candidates-age-out-projection
                  output-directory
                  (str random-seed))))
