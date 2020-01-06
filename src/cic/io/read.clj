(ns cic.io.read
  (:require [clojure.core.async :as a]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.set :as cs]
            [clojure.string :as str]
            [camel-snake-kebab.core :as csk]
            [clj-time.format :as f]))

(defn blank-row? [row]
  (every? str/blank? row))

(def date-format
  (f/formatter :date))

(defn parse-date
  [s]
  (f/parse date-format s))

(defn parse-double
  [d]
  (Double/parseDouble d))

(defn parse-int
  [i]
  (int (Double/parseDouble i)))

(defn parse-placement
  [s]
  (let [placement (keyword s)]
    (cond
      (#{:U1 :U2 :U3} placement) :Q1
      (#{:U4 :U5 :U6} placement) :Q2
      :else placement)))

(defn parse-ceased [s]
  (cond
    str/blank? nil
    #(= % "NA") nil
    :else (parse-date s)))

(defn format-episode
  [row]
  (try
    (-> (cs/rename-keys row {:id :child-id :care-status :CIN})
        (update :dob #(Long/parseLong %))
        (update :report-date parse-date)
        (update :ceased parse-ceased)
        (update :report-year #(Long/parseLong %))
        (update :placement parse-placement)
        (update :CIN keyword)
        (update :legal-status keyword)
        (update :uasc (comp boolean #{"True"})))
    (catch Exception e
      {:error (ex-message e)
       :data row})))

(defn load-csv
  "Loads csv file with each row as a vector.
   Stored in map separating column-names from data"
  [filename]
  (let [parsed-csv (with-open [in-file (io/reader filename)]
                     (->> in-file
                          data-csv/read-csv
                          (remove blank-row?)
                          (vec)))
        parsed-data (rest parsed-csv)
        headers (map csk/->kebab-case-keyword (first parsed-csv))]
    (map (partial zipmap headers) parsed-data)))

(defn episodes
  [filename]
  (->> (load-csv filename)
       (map format-episode)))

(defn duration-csv
  "Helper function for duration-csvs"
  [filename]
  (let [parsed-csv (with-open [in-file (io/reader filename)]
                     (->> in-file
                          data-csv/read-csv
                          (remove blank-row?)
                          (vec)))
        parsed-data (rest parsed-csv)]
    (->> (map (comp (juxt first (comp vec rest)) (partial map parse-int)) parsed-data)
         (into {}))))

(defn duration-csvs
  "Loads three files containing the lower, median and upper bounds
  of duration in care by age of entry.
  Each of these files is output by a survival analysis in R."
  [lower-filename median-filename upper-filename]
  (let [lower (duration-csv lower-filename)
        median (duration-csv median-filename)
        upper (duration-csv upper-filename)]
    (->> (for [age (range 0 19)]
           (let [lower (get lower age)
                 median (get median age)
                 upper (get upper age)]
             (vector age
                     (mapv (fn [l m u]
                             (vector l m u))
                           lower median upper))))
         (into {}))))

(defn joiner-csvs
  "Loads the two files required for projecting joiners.
  The first is a sample of coefficients from the GLM (see coef.samples in joiners.R)
  The second is an inference of the gamma dispersion by age (see gamma.rates in joiners.R)
  although currently the same dispersion is reported for all ages."
  [coefs-filename dispersion-filename]
  (let [model-coefs (->> (load-csv coefs-filename)
                         (first) ;; We're only interested in the first row
                         (reduce-kv (fn [coll k v] (assoc coll k (parse-double v))) {}))
        gamma-params (->> (load-csv dispersion-filename)
                          (map #(-> %
                                    (update :admission-age parse-int)
                                    (update :shape parse-double)
                                    (update :rate parse-double)
                                    (update :dispersion parse-double)))
                          (map (juxt :admission-age identity))
                          (into {}))]
    {:model-coefs model-coefs :gamma-params gamma-params}))

(defn costs-csv
  [filename]
  (->> (load-csv filename)
       (map #(-> %
                 (update :placement keyword)
                 (update :cost parse-double)))))

(defn csv->results
  "tap-map is a map of functions that takes a mult and returns channels via tap/into/reduce/transduce/etc"
  [in-file-name in-xf tap-map]
  (let
      [in-chan (a/chan 512 in-xf)
       record-mult (a/mult in-chan)
       tap-map (into {}
                     (map (fn [[k f]] [k (f record-mult)]))
                     tap-map)]

    (with-open [r (io/reader in-file-name)]
      (try
        (run!
         (fn [rec]
           (tap> {:level :debug :msg "Sending a line! " :data (first rec)})
           (a/>!! in-chan rec)
           (tap> {:level :debug :msg "Sent a line! " :data (first rec)}))
         (data-csv/read-csv r))
        (catch Exception e (tap> {:level :error
                                  :data (ex-info "Unexpected Error" {:file in-file-name} e)}))
        (finally (a/close! in-chan))))
    (into {}
          (map (fn [[k result-channel]] [k (a/<!! result-channel)]))
          tap-map)))

(comment

  (defn debug-log [m]
    (prn m))

  (add-tap debug-log)
  (remove-tap debug-log)


  (def episodes-scrubbed-header
    [:row-id :child-id :report-date :ceased :legal-status :care-status :placement :report-year :sex :dob :period-id :episode-number :phase-number :phase-id])

  (def episodes-xf
    (comp
     (drop 1)
     (map (fn [r] (zipmap episodes-scrubbed-header r)))
     (map format-episode)))

  (def bar
    (csv->results "/home/bld/wip/cic/witan.csc.cambridgeshire/data/episodes.scrubbed.csv"
                  episodes-xf
                  {:first-one (fn [record-mult] (a/tap record-mult (a/chan 32 (take 1))))
                   :episodes (fn [record-mult] (a/into [] (a/tap record-mult (a/chan 512 (filter (complement :error))))))
                   :error-episodes (fn [record-mult] (a/into [] (a/tap record-mult (a/chan 512 (filter :error)))))}))

  (def foo
    (time
     (let
         ;; define transforms, reduces, and intos in the let
         [episodes-chan (a/chan 512 episodes-xf)
          episodes-chan-mult (a/mult episodes-chan)
          well-formatted-episodes (a/into [] (a/tap episodes-chan-mult (a/chan 512 (filter (complement :error)))))
          error-episodes (a/into [] (a/tap episodes-chan-mult (a/chan 512 (filter :error))))
          first-one (a/tap episodes-chan-mult (a/chan 32 (take 1)))
          ]

       ;; put data onto the channel
       (with-open [r (io/reader "/home/bld/wip/cic/witan.csc.cambridgeshire/data/episodes.scrubbed.csv")]
         (run!
          #(do (tap> {:level :debug :msg "Sending a line! " :data (first %)})
               (a/>!! episodes-chan %)
               (tap> {:level :debug :msg "Sent a line! " :data (first %)}))
          (data-csv/read-csv r))
         (a/close! episodes-chan))

       ;; do blocking things to get the data out here
       {:episodes (a/<!! well-formatted-episodes)
        :error-episodes (a/<!! error-episodes)
        :first-one (a/<!! first-one)})))

  (require '[clojure.data])
  (clojure.data/diff bar foo)

  (first foo)
  (format-episode (first foo))
  (->> foo
       (map format-episode)
       (filter :error))

  )
