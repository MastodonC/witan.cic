(ns cic.core
  (:require [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]))

(defn blank-row? [row]
  (every? #(= "" %) row))

(defn load-csv
  "Loads csv file with each row as a vector.
   Stored in map separating column-names from data"
  ([filename]
   (let [parsed-csv (with-open [in-file (io/reader filename)]
                      (into [] (->> in-file
                                    data-csv/read-csv
                                    (remove (fn [row] (blank-row? row))))))
         parsed-data (rest parsed-csv)
         headers (map keyword (first parsed-csv))]
     (map (fn [row] (reduce into {} (map (fn [k v] (assoc {} k v)) headers row))) parsed-data))))

(defn remove-UASCs-and-V4s [data]
  (remove #(or (= "V4" (:legal_status %)) (= true (:UASC %))) data))

(defn -main [filename]
  (let [data (load-csv filename)]
    (remove-UASCs-and-V4s data)))
