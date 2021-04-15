(ns cic.main
  (:gen-class)
  (:require [cic.cic :as cic]
            [cic.time :as time]
            [clojure.java.io :as io]
            [aero.core :as aero]))

(defn read-config
  [config-file]
  (binding [*data-readers* {'date time/string-as-date}]
    (aero/read-config config-file)))

(defn run-cic
  [config-file]
  (let [config (-> (read-config config-file)
                   (assoc :config-file config-file))]
    (cic/run-cic-workflow config)))

(defn -main
  ([] (-main "data/demo/config.edn"))
  ([config-file] (run-cic config-file)))
