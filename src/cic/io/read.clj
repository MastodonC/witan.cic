(ns cic.io.read
  (:require [camel-snake-kebab.core :as csk]
            [cic.time :as time]
            [clj-time.format :as f]
            [clojure.data.csv :as data-csv]
            [clojure.java.io :as io]
            [clojure.set :as cs]
            [clojure.string :as str]))

(defn blank-row? [row]
  (every? str/blank? row))

(def date-format
  (f/formatter :date))

(defn parse-date
  [s]
  (f/parse date-format s))

(def month-format
  (f/formatter "YYYY-MM"))

(defn parse-month
  [s]
  (f/parse month-format s))

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

(defn format-episode
  [row]
  (-> (cs/rename-keys row {:id :child-id :care-status :CIN :dob :birth-month})
      (update :birth-month parse-month)
      (update :report-date parse-date)
      (update :ceased #(when-not (str/blank? %) (parse-date %)))
      (update :report-year #(Long/parseLong %))
      (update :placement parse-placement)
      (update :CIN keyword)
      (update :legal-status keyword)
      (update :uasc (comp boolean #{"True"}))))

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
    (into [] (map (partial zipmap headers)) parsed-data)))

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

(defn joiner-csv
  "Loads the expected joiners numbers"
  [joiners-csv]
  (let [coefs (->> (load-csv joiners-csv)
                   (map #(-> %
                             (update :param parse-double)))
                   (map (juxt :name :param))
                   (into {}))]
    (if (empty? coefs)
      (throw (ex-info "Tried to load empty joiner csv"
                      {:model-coefs coefs :file joiners-csv}))
      {:model-coefs coefs})))

(defn costs-csv
  [filename]
  (->> (load-csv filename)
       (map #(-> %
                 (update :placement keyword)
                 (update :cost parse-double)))))

(defn phase-durations-csv
  [filename]
  (->> (load-csv filename)
       (reduce (fn [m {:keys [label param]}]
                 (assoc m (-> label str/lower-case keyword) {:lambda (parse-double param)}))
               {})))

(defn phase-duration-quantiles-csv
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :quantile parse-int)
                  (update :value parse-double))))
       (reduce (fn [m {:keys [row label value]}]
                 (update m (keyword label) (fnil conj []) value))
               {})))

(defn phase-transitions-csv
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :first-transition = "TRUE")
                  (update :transition-age parse-int)
                  (update :transition-to keyword)
                  (update :transition-from keyword)
                  (update :n parse-int)
                  (update :m parse-int))))
       (group-by #(select-keys % [:first-transition :transition-age :transition-from]))
       (reduce (fn [m [k v]]
                 (assoc m k (reduce (fn [m {:keys [transition-to n]}]
                                      (assoc m transition-to n)) {} v)))
               {})))

(defn joiner-placements-csv
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :admission-age parse-int)
                  (update :first-placement keyword)
                  (update :n parse-int))))
       (reduce (fn [m {:keys [admission-age first-placement n]}]
                 (assoc-in m [admission-age first-placement] n))
               {})))

(defn age-beta-params
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :age parse-int)
                  (update :alpha parse-double)
                  (update :beta parse-double))))
       (reduce (fn [m {:keys [age alpha beta]}]
                 (assoc m age {:alpha alpha :beta beta}))
               {})))

(defn placement-csvs
  [joiner-placements phase-durations phase-transitions phase-duration-quantiles
   phase-bernoulli-params phase-beta-params]
  {:joiner-placements (joiner-placements-csv joiner-placements)
   :phase-transitions (phase-transitions-csv phase-transitions)
   :phase-duration-quantiles (phase-duration-quantiles-csv phase-duration-quantiles)
   :phase-bernoulli-params (age-beta-params phase-bernoulli-params)
   :phase-beta-params (age-beta-params phase-beta-params)})

(defn survival-hazard
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :admission-age parse-int)
                  (update :duration parse-double)
                  (update :hazard parse-double))))
       (group-by :admission-age)))

(defn knn-closed-cases
  [filename]
  (->> (load-csv filename)
       (remove (fn [{:keys [closed]}] (= closed "NULL")))
       (map (fn [row]
              (-> row
                  (update :offset parse-int))))
       (group-by :open)))

(defn rejection-proportions
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :age parse-int)
                  (update :group parse-double)
                  (update :p parse-double))))
       (group-by (juxt :age :group))
       (reduce (fn [coll [k v]]
                 (assoc coll k (reduce (fn [coll {:keys [provenance p]}]
                                         (assoc coll (keyword (str/lower-case provenance)) p))
                                       {} v)))
               {})))

(defn returning
  [x & args]
  x)

(defn period-candidates
  [filename]
  (println "Reading period candidates")
  (returning
   (->> (load-csv filename)
        (map (fn [row]
               (try
                 (-> row
                     (update :admission-age parse-int)
                     (update :admission-age-days parse-int)
                     (update :duration parse-int)
                     (update :duration-group parse-int)
                     (update :reject-ratio parse-double))
                 (catch Exception e
                   (println row)
                   (throw e))))))
   (println "Read period candidates")))

(defn age-out-proportions
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :admission-age parse-int)
                  (update :current-age-days parse-int)
                  (update :p parse-double))))
       (group-by :admission-age)
       (reduce (fn [coll [admission-age rows]]
                 (let [rows (sort-by :current-age-days rows)
                       pairs (map (juxt :current-age-days :p) rows)]
                   (assoc coll admission-age {:marginal (-> rows first :p)
                                              :joint-pairs (reverse pairs)
                                              :joint-indexed (into {} pairs)})))
               {})))

(defn age-out-candidates
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (-> row
                  (update :admission-age parse-int)
                  (update :admission-age-days parse-int)
                  (update :duration parse-int))))))

(defn scenario-joiner-rates
  [filename]
  (->> (load-csv filename)
       (map (fn [row]
              (reduce (fn [coll x]
                        (update coll x parse-double))
                      (update row :date parse-date)
                      (map (comp keyword str) (range 18)))))
       (sort-by :date)))
