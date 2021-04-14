(ns cic.cic
  (:require [cic.repl :as repl]))

(defn run-cic-workflow
  [config]
  (let [{:keys [simulations random-seed rewind-years train-years project-years episodes-extract-date train-joiner-range]} (:projection-parameters config)]
    (repl/generate-projection-csv! rewind-years train-years project-years simulations random-seed)))
