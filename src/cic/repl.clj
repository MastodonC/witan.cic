(ns cic.repl
  (:require [cic.episodes :as episodes]
            [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.model :as model]
            [cic.periods :as periods]
            [cic.projection :as projection]
            [cic.random :as rand]
            [cic.summary :as summary]
            [cic.time :as time]
            [cic.validate :as validate]
            [clojure.set :as cs]
            [net.cgrand.xforms :as xf]
            [net.cgrand.xforms.rfs :as xrf]
            [redux.core :as rx]
            [kixi.stats.core :as k]))

  (def ccc "data/ccc/2020-06-09/%s")
  (def ncc "data/ncc/2020-06-09/%s")
  (def scc "data/scc/2020-12-04/%s")

(def input-format
  scc)

(def input-file (partial format input-format))
(def output-file (partial format input-format))
(defn la-label []
  (last (re-find #"/([a-z]+cc)/" input-format)))

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
  ([{:keys [episodes-csv placement-costs-csv duration-lower-csv duration-median-csv duration-upper-csv
            zero-joiner-day-ages-csv survival-hazard-csv]}]
   (let [episodes (-> (read/episodes episodes-csv)
                      (episodes/remove-f6))
         latest-event-date (->> (mapcat (juxt :report-date :ceased) episodes)
                                (keep identity)
                                (time/max-date))]
     (hash-map :latest-event-date latest-event-date
               :periods (periods/from-episodes episodes)
               :placement-costs (read/costs-csv placement-costs-csv)
               ;; :knn-closed-cases (read/knn-closed-cases knn-closed-cases-csv)
               :joiner-birthday-model (-> (read/zero-joiner-day-ages zero-joiner-day-ages-csv)
                                          (model/joiner-birthday-model)))))
  ([]
   (load-model-inputs {:episodes-csv (input-file "suffolk-scrubbed-episodes-20201203.csv")
                       :placement-costs-csv (input-file "placement-costs.csv")
                       ;; :duration-lower-csv (input-file "duration-model-lower.csv")
                       ;; :duration-median-csv (input-file "duration-model-median.csv")
                       ;; :duration-upper-csv (input-file "duration-model-upper.csv")
                       :zero-joiner-day-ages-csv (input-file "zero-joiner-day-ages.csv")
                       ;; :survival-hazard-csv (input-file "survival-hazard.csv")
                       ;; :knn-closed-cases-csv (input-file "knn-closed-cases.csv")
                       })))

(defn prepare-model-inputs
  [{:keys [latest-event-date periods] :as model-inputs} rewind-years]
  (let [project-from (time/years-before latest-event-date rewind-years)
        periods (->> (periods/periods-as-at periods project-from)
                     (periods/assoc-birthday-bounds))]
    (assoc model-inputs
           :project-from project-from
           :periods periods)))

(defn format-actual-for-output
  [[date summary]]
  (-> (assoc summary :date date)
      (cs/rename-keys {:count :actual
                       :cost :actual-cost})
      (update :placements #(into {} (map (fn [[k v]] (vector k {:median v}))) %))
      (update :ages #(into {} (map (fn [[k v]] (vector k {:median v}))) %))
      (update :placement-ages #(into {} (map (fn [[k v]] (vector k {:median v}))) %))))

(defn generate-projection-csv!
  "Main REPL function for writing a projection CSV"
  [rewind-years train-years project-years n-runs seed]
  (let [{:keys [project-from periods placement-costs duration-model joiner-birthday-model]} (prepare-model-inputs (load-model-inputs) rewind-years)
        _ (println (str "Project from " project-from))
        output-file (output-file (format "%s-projection-%s-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s-euclidean-quantile-1-7interval.csv" (la-label) (time/date-as-string project-from) rewind-years train-years project-years n-runs seed))
        ;; project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        model-seed {:periods periods
                    :duration-model duration-model
                    ;; :knn-closed-cases knn-closed-cases
                    :learn-from learn-from
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]
                    :segments-range [(time/years-before learn-from 20) (time/years-after project-from 20)]
                    :project-from project-from
                    :project-to project-to}
        output-from (time/years-before learn-from 2)
        summary-seq (into []
                          (map format-actual-for-output)
                          (summary/periods-summary (rand/sample-birthdays periods (rand/seed seed))
                                                   (time/day-seq output-from project-from 7)
                                                   placement-costs))
        projection (projection/projection model-seed
                                          (time/day-seq project-from project-to 7)
                                          placement-costs
                                          seed n-runs)]
    (->> (write/projection-table (concat summary-seq projection))
         (write/write-csv! output-file))))

(defn generate-distribution-csv!
  "Main REPL function for writing a projection CSV"
  [rewind-years train-years n-samples seed]
  (let [{:keys [project-from periods placement-costs duration-model joiner-birthday-model]} (prepare-model-inputs (load-model-inputs) rewind-years)
        _ (println (str "Project from " project-from))
        output-file (output-file (format "%s-distribution-%s-segment-interval-%s-rewind-%syr-train-%syr-samples-%s-seed-%s.csv" (la-label) (time/date-as-string project-from) periods/segment-interval rewind-years train-years n-samples seed))
        ;; project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        periods (rand/sample-birthdays periods (rand/seed seed))
        learn-from (time/years-before project-from train-years)
        period-completer (model/markov-placements-model periods learn-from project-from true)
        simulated-periods (into []
                                (comp (map period-completer)
                                      (map #(assoc % :provenance "S"))
                                      (take n-samples))
                                (periods/joiner-generator periods))]
    (->> (periods/period-generator periods project-from)
         (into simulated-periods
               (comp (map period-completer)
                     (map #(assoc % :provenance "P"))
                     (take n-samples)))
         (write/duration-table)
         (write/write-csv! output-file))))

(defn output-segments-csv!
  [rewind-years train-years seed]
  (let [{:keys [project-from periods placement-costs duration-model joiner-birthday-model]} (prepare-model-inputs (load-model-inputs) rewind-years)
        _ (println (str "Project from " project-from))
        output-file (output-file (format "%s-segments-%s-segment-interval-%s-rewind-%syr-train-%syr-seed-%s.csv" (la-label) (time/date-as-string project-from) periods/segment-interval rewind-years train-years seed))
        ;; project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        periods (rand/sample-birthdays periods (rand/seed seed))
        learn-from (time/years-before project-from train-years)
        close-open-periods? true
        offset-segments (model/offset-groups periods learn-from project-from close-open-periods?)]
    (->> (vals offset-segments)
         (apply concat)
         (write/segments-table)
         (write/write-csv! output-file))))

(defn generate-annual-csv!
  [output-file rewind-years train-years project-years n-runs seed]
  (let [{:keys [periods placement-costs duration-model joiner-birthday-model]} (load-model-inputs)
        project-from (time/days-after (time/financial-year-end (time/max-date (map :beginning periods))) rewind-years)
        project-to (time/financial-year-end (time/years-after project-from project-years))
        learn-from (time/years-before project-from train-years)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        costs-lookup (into {} (map (juxt :placement :cost) placement-costs))
        actuals-by-year-age (into {} (comp (filter #(time/< (:beginning %) project-from))
                                           (xf/by-key (juxt (comp time/year time/financial-year-end :beginning) :admission-age)
                                                      (xf/reduce k/count)))
                                  (rand/sample-birthdays periods (rand/seed seed)))
        actuals-by-year (into {} (xf/by-key ffirst second (xf/reduce +)) actuals-by-year-age)
        actuals (->> (reduce (fn [coll [year joiners]]
                               (-> (assoc-in coll [year :actual-joiners] joiners)
                                   (assoc-in [year :year] year)))
                             (reduce (fn [coll [[year age] joiners]]
                                       (assoc-in coll [year :joiners-ages age] joiners))
                                     {} actuals-by-year-age)
                             actuals-by-year)
                     (vals))
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        output-from (time/years-before learn-from 2)
        _(println project-from project-to learn-from output-from)
        cost-projection (-> (into []
                                  (filter #(< (time/year project-from)
                                              (:year %)
                                              (time/year project-to)))
                                  (projection/cost-projection projection-seed
                                                              model-seed
                                                              project-to
                                                              placement-costs
                                                              seed n-runs))
                            (into actuals))]
    (->> (write/annual-report-table cost-projection)
         (write/write-csv! output-file))))

(defn period->placement-seq
  "Takes a period and returns the sequence of placements as AA-BB-CC.
  Consecutive episodes in the same placement are collapsed into one."
  [{:keys [episodes] :as period}]
  (transduce
   (comp (map :placement)
         (partition-by identity)
         (map (comp name first))
         (interpose "-"))
   xrf/str
   episodes))

(defn generate-placement-sequence-csv!
  [output-file train-years project-years n-runs seed]
  (let [{:keys [periods placement-costs duration-model joiner-birthday-model]} (load-model-inputs)
        project-from (time/max-date (map :beginning periods))
        project-to (time/financial-year-end (time/years-after project-from project-years))
        learn-from (time/years-before project-from train-years)
        closed-periods (remove :open? periods)
        projection-seed {:seed (filter :open? periods)
                         :date project-from}
        model-seed {:seed periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]}
        output-from (time/years-before learn-from 2)]
    (let [freduce (partial xf/into [] (xf/by-key (juxt :admission-age period->placement-seq) xf/count))
          age-summary (partial xf/into {} (xf/by-key ffirst second (xf/reduce +)))
          age-sequence-totals (->> (rand/split-n (rand/seed seed) n-runs)
                                   (pmap (fn [seed]
                                           (freduce (projection/project-1 projection-seed model-seed project-to seed))))
                                   (apply concat)
                                   (into {} (xf/by-key (xf/reduce +))))
          actual-age-sequence-totals (freduce (rand/sample-birthdays closed-periods (rand/seed seed)))
          age-totals (age-summary age-sequence-totals)]
      (->> {:projected-age-sequence-totals age-sequence-totals
            :projected-age-totals age-totals
            :actual-age-sequence-totals actual-age-sequence-totals
            :actual-age-totals (age-summary actual-age-sequence-totals)}
           (write/placement-sequence-table)
           (write/write-csv! output-file)))))

(defn generate-validation-csv!
  "Outputs model projection and linear regression projection together with actuals for comparison."
  [train-years n-runs seed]
  (let [rewind-years 1
        project-years 1
        output-file (output-file (format "validation-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s.csv" rewind-years train-years project-years n-runs seed))
        {:keys [project-from periods placement-costs duration-model joiner-birthday-model]} (prepare-model-inputs (load-model-inputs))
        project-from (time/quarter-preceding (time/years-before project-from rewind-years))
        _ (println (str "Project from " project-from))
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        projection-periods (periods/periods-as-at periods project-from)
        projection-seed {:seed (filter :open? projection-periods)
                         :date project-from}
        model-seed {:seed projection-periods
                    :duration-model duration-model
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]
                    :project-to project-to}
        projection (->> (projection/project-n projection-seed model-seed [project-to] seed n-runs)
                        (map #(summary/periods-summary % [project-to] placement-costs))
                        (summary/grand-summary)
                        (first))
        actuals (-> (summary/periods-summary (rand/sample-birthdays periods (rand/seed seed))
                                             [project-to]
                                             placement-costs)
                    (first)
                    (format-actual-for-output))]
    (->> (validate/compare-projected projection actuals)
         (write/validation-table)
         (write/write-csv! output-file))))

(defn generate-episodes-csv!
  "Outputs a file showing a single projection in rowise episodes format."
  [rewind-years train-years project-years n-runs seed]
  (let [{:keys [project-from periods placement-costs duration-model joiner-birthday-model knn-closed-cases] :as model-inputs} (prepare-model-inputs (load-model-inputs) rewind-years)
        _ (println (str "Project from " project-from))
        output-file (output-file (format "%s-episodes-%s-rewind-%syr-train-%syr-project-%syr-runs-%s-seed-%s-segments-range.csv" (la-label) (time/date-as-string project-from) rewind-years train-years project-years n-runs seed))
        _ (println output-file)
        project-to (time/years-after project-from project-years)
        learn-from (time/years-before project-from train-years)
        t0 (time/min-date (map :beginning periods))
        model-seed {:periods periods
                    :joiner-birthday-model joiner-birthday-model
                    :joiner-range [learn-from project-from]
                    :episodes-range [learn-from project-from]
                    :segments-range [learn-from project-from]
                    :project-to project-to
                    :project-from project-from}]
    (->> (projection/project-n model-seed [project-to] seed n-runs)
         (write/episodes-table t0 project-to)
         (write/write-csv! output-file))))

(comment
  
  (def episodes (read/episodes (input-file "episodes.scrubbed.csv")))
  (def periods (periods/from-episodes episodes))
  (def project-from (-> (->> (mapcat (juxt :report-date :ceased) episodes)
                             (keep identity)
                             (time/max-date))
                        (time/years-before 1)))
  (def periods (periods/assoc-birthday-bounds (map #(assoc % :snapshot-date project-from) periods)))
  (def periods (rand/sample-birthdays periods (rand/seed 42)))
  (def periods (periods/periods-as-at periods project-from))
  (println (filter #(when (= (:period-id %) "467-1") %) periods))
  (def knn-closed-cases (read/knn-closed-cases (input-file "knn-closed-cases.csv")))
  (def markov-model (model/markov-placements-model periods))
  (markov-model (first (filter #(= (:period-id %) "2409-2") periods)))
  (filter #(= (:period-id %) "861-3") periods)
  (filter #(= (:period-id %) "2526-1") periods)
  (filter #(= (:open %) "861-3") knn-closed-cases))


(comment
  (def periods-csv (write/periods->knn-closed-cases-csv periods))

  (print periods-csv)

  project-from

  (def offset-groups (clojure.edn/read-string (slurp "offset-groups.edn")))

  (def offset-groups-list (mapcat (fn [[offset v]] (mapcat (fn [[k v]] (map (fn [v] (assoc v :offset offset)) v)) v)) offset-groups))

  (first offset-groups-list)

  (spit "offset-groups.csv"
        (str "id,from-placement,to-placement,age,terminal,duration,offset\n"
             (->> (for [{:keys [id from-placement to-placement age terminal? duration offset]} offset-groups-list]
                    (clojure.string/join "," (vector id from-placement to-placement age terminal? duration offset)))
                  (clojure.string/join "\n"))))
  )

(comment
  (def offset-segments (model/offset-groups periods
                                            (time/years-before project-from 10)
                                            (time/years-after project-from 10)
                                            true))
  
  (def placements (distinct (map :placement episodes)))

  (def source-segments
    (into [] (comp (mapcat (fn [placement]
                             (get offset-segments [0 true placement true])))
                   (filter (every-pred :initial? :terminal?))) placements))

  (def max-age-days (* 365 18))

  (def get-matched-segment (fn [feature-fn feature-vec segments]
                             (model/min-key'
                              (fn [segment]
                                (model/euclidean-distance (feature-fn segment) feature-vec))
                              segments)))

  (def jitter-scale 1)

  (defn to-table [cols xs]
    (into [(mapv name cols)]
          (map (apply juxt cols))
          xs))


  (def period-simulations
    (into [] (comp (filter :open?)
                   (map (fn [{:keys [birthday beginning snapshot-date duration episodes] :as period}]
                          (assoc period
                                 :join-age-days (time/day-interval birthday beginning)
                                 :age-days (time/day-interval birthday snapshot-date)
                                 :care-days (time/day-interval beginning snapshot-date)
                                 :max-duration (dec (time/day-interval beginning (time/days-after birthday max-age-days)))
                                 :offset (rem duration periods/segment-interval)
                                 :last-placement (-> episodes last :placement)
                                 :initial? (< duration periods/segment-interval))))
                   (mapcat (fn [{:keys [age-days care-days duration max-duration offset last-placement initial? join-age-days] :as period}]
                             (into [] (map (fn [simulation]
                                             (let [age-days (model/jitter-binomial age-days max-age-days jitter-scale)
                                                   care-days (model/jitter-binomial duration max-duration jitter-scale)
                                                   segment (get-matched-segment (juxt :age-days :care-days) [age-days care-days]
                                                                                (get offset-segments [offset true last-placement initial?]))]
                                               (when segment
                                                 (assoc period
                                                        :combined-duration (+ duration (:duration segment))
                                                        :care-weeks (quot (+ duration (:duration segment)) 7)
                                                        :segment-simulation simulation
                                                        :segment-terminal? (:terminal? segment)
                                                        :segment-duration (:duration segment)
                                                        :segment-age-days (:age-days segment)
                                                        :segment-care-days (:care-days segment)
                                                        :last-placement (-> segment :episodes last :placement)
                                                        :segment-aged-out? (:aged-out? segment))))))
                                   (range 100))))
                   (remove nil?))
          periods))

  (cic.io.write/write-csv! "period-simulations.csv" (to-table [:join-age-days :care-days :last-placement :segment-duration :segment-terminal?] period-simulations))

  (cic.io.write/write-csv! "closed-periods.csv" (to-table [:join-age-days :care-days :last-placement] (into [] (comp (remove :open?)
                                                                                                                     (map (fn [{:keys [birthday beginning end episodes] :as period}]
                                                                                                                            (assoc period
                                                                                                                                   :join-age-days (time/day-interval birthday beginning)
                                                                                                                                   :care-days (time/day-interval beginning end)
                                                                                                                                   :last-placement (-> episodes last :placement)))))
                                                                                                            periods)))

  ;; For terminal durations less than 1 year, what is the density per week?

  (def inc! (fnil inc 0))

  (def simulated-distribution
    (->> (into []
               (filter (every-pred :segment-terminal? #(< (:combined-duration %) periods/segment-interval)))
               period-simulations)
         (reduce (fn [acc {:keys [admission-age care-weeks] :as simulation}]
                   (-> acc
                       (update-in [admission-age care-weeks] inc!)
                       (update-in [admission-age :n] inc!)))
                 {})))


  (def empirical-distribution
    (->> (into []
               (comp (remove :open?)
                     (filter #(< (:duration %) periods/segment-interval))
                     (map #(assoc % :care-weeks (quot (:duration %) 7))))
               periods)
         (reduce (fn [acc {:keys [admission-age care-weeks] :as simulation}]
                   (-> acc
                       (update-in [admission-age care-weeks] inc!)
                       (update-in [admission-age :n] inc!)))
                 {})))
  

  
  (defn admission-age-care-weeks-pdf
    [sample]
    (let [counts (reduce (fn [acc {:keys [admission-age care-weeks] :as simulation}]
                           (-> acc
                               (update-in [admission-age care-weeks] inc!)
                               (update-in [admission-age :n] inc!)))
                         {})]
      (reduce (fn [counts age]
                (let [n (get-in counts [age :n] 0)
                      +' (fnil + 0)
                      adj (/ 1 52)]
                  (reduce (fn [counts week]
                            (-> counts
                                (update-in [age week] +' adj)
                                (update-in [age week] / (inc n))
                                (update-in [age week] double)))
                          (update counts age dissoc :n)
                          (range 52))))
              counts
              (range 17))))
  


  )


