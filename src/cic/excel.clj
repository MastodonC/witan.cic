(ns witan.cic.excel
  (:require [dk.ative.docjure.spreadsheet :as xl]))

(defn read-row [^org.apache.poi.ss.usermodel.Row row]
  (map xl/read-cell (xl/cell-seq row)))

(defn rows [file-name sheet-name]
  (let [row-seq (->> (xl/load-workbook file-name)
                     (xl/select-sheet sheet-name)
                     xl/row-seq)]
    (map read-row row-seq)))
