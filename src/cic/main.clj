(ns cic.main
  (:gen-class)
  (:require [cic.cic :as cic]
            [cic.time :as time]
            [clojure.java.io :as io]
            [aero.core :as aero]
            [clojure.tools.cli :as cli]))

(defn read-config
  [config-file]
  (binding [*data-readers* {'date time/string-as-date}]
    (aero/read-config config-file)))

(defn run-cic
  [config-file]
  (let [config (-> (read-config config-file)
                   (assoc :config-file config-file))]
    (cic/run-cic-workflow config)))

(defn run-generate-candidates
  [config-file]
  (let [config (-> (read-config config-file)
                   (assoc :config-file config-file))]
    (cic/run-generate-candidates-workflow config)))

(defn run-rejection-sampling
  [config-file]
  (let [config (-> (read-config config-file)
                   (assoc :config-file config-file))]
    (cic/run-rejection-sampling config)))

(def cli-options
  [
   ["-c" "--config FILE" "Config file"
    :default "data/demo/config.edn"
    :id :config-file]])

(defn -main
  [& args]
  (let [{:keys [options arguments]} (cli/parse-opts args cli-options)
        {:keys [config-file]} options
        [task] arguments]
    (case task
      "generate-candidates" (run-generate-candidates config-file)
      "rejection-sampling" (run-rejection-sampling config-file)
      "projection" (run-cic config-file)
      nil (run-cic config-file))))
