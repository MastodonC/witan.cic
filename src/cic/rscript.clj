(ns cic.rscript
  "Namespace for executing R scripts and managing data (de)serialisation"
  (:require [cic.io.write :as write]
            [clj-time.format :as f]
            [clojure.java.shell :as shell]
            [clojure.set :as cs]
            [clojure.string :as str]
            [taoensso.timbre :as log]))

(defn exec
  [script-path & args]
  (log/info (format "Executing %s %s" script-path (str/join " " args)))
  (let [return-val (apply shell/sh "Rscript" "--vanilla" script-path args)]
    ;; rscript is quite chatty, so only pass on err text if exit was abnormal
    (if (not= 0 (:exit return-val))
      (do (log/error script-path args)
          (log/error return-val))
      (log/info (format "Executed %s" script-path)))))

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
