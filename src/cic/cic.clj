(ns cic.cic
  (:require [cic.repl :as repl]))

(defn run-cic-workflow
  [config]
  (repl/generate-projection-csv! config))
