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

(def cli-options
  [
   ["-c" "--config FILE" "Config file"
    :default "data/demo/config.edn"
    :id :config-file]])

(defn -main
  [& args]
  (let [{:keys [options arguments]} (cli/parse-opts args cli-options)
        {:keys [config-file]} options
        [task] arguments
        config (-> (read-config config-file)
                   (assoc :config-file config-file))]
    (case task
      "generate-candidates" (cic/run-generate-candidates-workflow config)
      "rejection-sampling" (cic/run-rejection-sampling config)
      "projection" (cic/run-cic-workflow config)
      nil (cic/run-cic-workflow config))))
