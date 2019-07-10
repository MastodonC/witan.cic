(ns witan.cic.episodes
  (:require [witan.cic.excel :as ex]))

(defn join-episodes [filename]
  (let [sheets (->> (xl/load-workbook filename)
                    (xl/sheet-seq))
        episodes_header [:id :report_date :care_status :placement :ceased :legal_status]
        data (->> (ex/rows filename sheetname)
                  rest)]))
