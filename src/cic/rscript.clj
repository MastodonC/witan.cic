(ns cic.rscript
  "Namespace for executing R scripts and managing data (de)serialisation"
  (:require [cic.io.write :as write]
            [clj-time.format :as f]
            [clojure.java.shell :as shell]
            [clojure.set :as cs]))

(defn exec
  [script-path & args]
  (println (format "Executing %s" script-path))
  (let [return-val (apply shell/sh "Rscript" "--vanilla" script-path args)]
    ;; rscript is quite chatty, so only pass on err text if exit was abnormal
    (when (not= 0 (:exit return-val))
      (apply println script-path args)
      (println (:err return-val)))
    (println (format "Executed %s" script-path))))

(def date-format
  (f/formatter :date-hour-minute-second))

(defn date->str
  [date]
  (f/unparse date-format date))

(defn write-periods!
  [periods]
  (->> periods
       (map #(-> %
                 (select-keys [:admission-age :beginning])
                 (update :beginning date->str)
                 (cs/rename-keys {:admission-age :admission_age})))
       (write/mapseq->csv!)))

(defn write-phase-durations!
  [phase-durations]
  (write/mapseq->csv! phase-durations))
