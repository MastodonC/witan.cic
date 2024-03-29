(ns cic.repl
  (:require [clojure.core.async :as a]
            ;; Required for tagged literals in edn output
            [clj-time.coerce :as c]
            [cic.episodes :as episodes]
            [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.model :as model]
            [cic.periods :as periods]
            [cic.projection :as projection]
            [cic.random :as rand]
            [cic.summary :as summary]
            [cic.time :as time]
            [clojure.set :as cs]
            [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [me.raynes.fs :as fs]
            [net.cgrand.xforms :as xf]
            [net.cgrand.xforms.rfs :as xrf]
            [redux.core :as rx]
            [kixi.stats.core :as k]
            [taoensso.timbre :as log]))

  (def ccc "data/ccc/2020-06-09/%s")
  (def ncc "data/ncc/2020-06-09/%s")
  (def scc "data/scc/2021-04-08/%s")

  (def projection-label "latest-version-untrended")

(def input-format
  scc)

(def input-file (partial format input-format))
(def output-file (partial format input-format))
(defn la-label []
  (last (re-find #"/([a-z]+cc)/" input-format)))

(defn load-preparation-inputs
  ([{:keys [episodes-csv]}]
   (let [episodes (-> (read/episodes episodes-csv)
                      ;; FIXME: This should be done in scrubbing
                      (episodes/remove-f6))
         latest-event-date (->> (mapcat (juxt :report-date :ceased) episodes)
                                (keep identity)
                                (time/max-date))]
     (hash-map :latest-event-date latest-event-date
               :periods (periods/from-episodes episodes))))
  ([]
   (load-preparation-inputs {:episodes-csv (input-file "suffolk-scrubbed-episodes-20210219.csv")})))

(defn load-model-inputs
  "A useful REPL function to load the data files and convert them to  model inputs"
  ([{:keys [episodes age-out-proportions
            candidates-simulation candidates-projection
            candidates-age-out-projection candidates-age-out-simulation
            scenario-joiner-rates]}]
   (let [episodes (-> (read/episodes episodes)
                      ;; FIXME: This should be done in scrubbing
                      (episodes/remove-f6))
         latest-event-date (->> (mapcat (juxt :report-date :ceased) episodes)
                                (keep identity)
                                (time/max-date))]
     (cond-> (hash-map :periods (periods/from-episodes episodes)
                       ;; :knn-closed-cases (read/knn-closed-cases knn-closed-cases-csv)
                       :simulation-model
                       (-> (read/period-candidates candidates-simulation)
                           (model/simulation-model))
                       :projection-model
                       (-> (read/period-candidates candidates-projection)
                           (model/projection-model))
                       :age-out-model
                       (-> (read/age-out-proportions age-out-proportions)
                           (model/age-out-model))
                       :age-out-projection-model
                       (-> (read/age-out-candidates candidates-age-out-projection)
                           (model/age-out-projection-model))
                       :age-out-simulation-model
                       (-> (read/age-out-candidates candidates-age-out-simulation)
                           (model/age-out-simulation-model)))
       scenario-joiner-rates
       (assoc :scenario-joiner-rates (read/scenario-joiner-rates scenario-joiner-rates)))))
  ([]
   (load-model-inputs {:episodes (input-file "suffolk-scrubbed-episodes-20210219.csv")
                       :age-out-proportions (input-file "age-out-proportions.csv")
                       :candidates-simulation (input-file "simulated-candidates.csv")
                       :candidates-projection (input-file "projected-candidates.csv")
                       :candidates-age-out-projection (input-file "projected-age-out-candidates.csv")
                       :candidates-age-out-simulation (input-file "simulated-age-out-candidates.csv")})))

(defn prepare-periods
  [{:keys [periods] :as model-inputs} episodes-extract-date rewind-years]
  (let [project-from (time/years-before episodes-extract-date rewind-years)
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
  [{{:keys [rewind-years project-years simulations random-seed episodes-extract-date] {joiner-model-type :model joiner-train-years :train-years joiner-train-date-range :train-date-range} :joiners} :projection-parameters
    file-inputs :file-inputs
    output-parameters :output-parameters
    input-directory :input-directory
    output-directory :output-directory
    log-directory :log-directory
    config-file :config-file}]
  (let [model-inputs (load-model-inputs file-inputs)
        {:keys [project-from periods duration-model
                projection-model simulation-model
                age-out-model age-out-projection-model age-out-simulation-model
                scenario-joiner-rates]} (prepare-periods model-inputs episodes-extract-date rewind-years)
        ;; Output files
        projection-summary-output (fs/file output-directory "projection-summary.csv")
        projection-episodes-output (fs/file output-directory "projection-episodes.csv")
        projection-periods-output (fs/file output-directory "projection-periods.edn")
        historic-episodes-output (fs/file output-directory "historic-episodes.csv")
        ;; Log files
        joiner-rates-log-output (fs/file log-directory "joiner-rates-log.csv")
        joiner-scenario-log-output (fs/file log-directory "joiner-scenario-log.csv")
        joiner-interval-log-output (fs/file log-directory "joiner-interval-log.csv")
        ;;
        project-to (time/years-after project-from project-years)
        t0 (time/min-date (map :beginning periods))
        joiner-train-date-range (or joiner-train-date-range  [(time/years-before project-from joiner-train-years) project-from])
        model-seed {:periods periods
                    :duration-model duration-model
                    :joiner-range joiner-train-date-range
                    :project-from project-from
                    :project-to project-to
                    :projection-model projection-model
                    :simulation-model simulation-model
                    :age-out-model age-out-model
                    :age-out-projection-model age-out-projection-model
                    :age-out-simulation-model age-out-simulation-model
                    :joiner-model-type joiner-model-type
                    :scenario-joiner-rates scenario-joiner-rates}
        output-from (time/years-before (first joiner-train-date-range) 2)
        summary-seq (into []
                          (map format-actual-for-output)
                          (summary/periods-summary (rand/sample-birthdays periods (rand/seed random-seed))
                                                   (time/day-seq output-from project-from 7)))
        project-dates (time/day-seq project-from project-to 7)
        projection-chan (a/chan 1024)
        projection-mult (a/mult projection-chan)
        result-chan (a/chan 1024)
        to-complete (cond-> #{}
                      (:output-projection-summary? output-parameters)
                      (conj :projection-summary)
                      (:output-projection-episodes? output-parameters)
                      (conj :projection-episodes)
                      (:output-projection-periods? output-parameters)
                      (conj :projection-periods))]
    (fs/mkdir output-directory)
    (fs/mkdir log-directory)
    (when (:output-file-inputs? output-parameters)
      (fs/copy config-file (fs/file output-directory (fs/base-name config-file)))
      (fs/mkdir (fs/file output-directory "inputs"))
      (doseq [input-file (vals file-inputs)]
        (fs/copy input-file (fs/file output-directory "inputs" (fs/base-name input-file)))))
    (when (:output-historic-episodes? output-parameters)
      (let [{:keys [periods]} (prepare-periods model-inputs episodes-extract-date 0)
            periods (map #(assoc % :provenance "H") (rand/sample-birthdays periods (rand/seed random-seed)))]
        (->> (write/episodes-table t0 project-to [periods])
             (write/write-csv! historic-episodes-output))))
    (when (:output-projection-summary? output-parameters)
      (a/thread
        (->> (projection/projection projection-mult project-dates)
             (concat summary-seq)
             (write/projection-table)
             (write/write-csv! projection-summary-output))
        (a/>!! result-chan :projection-summary)))
    (when (:output-projection-episodes? output-parameters)
      (a/thread
        (->> (projection/project-n projection-mult)
             (write/episodes-table t0 project-to)
             (write/write-csv! projection-episodes-output))
        (a/>!! result-chan :projection-episodes)))
    (when (:output-projection-periods? output-parameters)
      (a/thread
        (->> (projection/project-n projection-mult)
             (write/write-edn! projection-periods-output))
        (a/>!! result-chan :projection-periods)))
    (fs/delete joiner-rates-log-output) ;; We're appending, so make sure we have a blank slate
    (fs/delete joiner-scenario-log-output)
    (fs/delete joiner-interval-log-output)
    (with-open [joiner-rates-writer (io/writer joiner-rates-log-output :append true)
                joiner-scenario-writer (io/writer joiner-scenario-log-output :append true)
                joiner-interval-writer (io/writer joiner-interval-log-output :append true)]
      (csv/write-csv joiner-rates-writer (vector write/joiner-rates-headers))
      (add-tap (fn [{:keys [message-type message]}]
                 (when (= message-type :joiners)
                   (csv/write-csv joiner-rates-writer (write/joiner-rates-tap message)))))
      (csv/write-csv joiner-scenario-writer (vector write/joiner-scenario-headers))
      (add-tap (fn [{:keys [message-type message]}]
                 (when (= message-type :scenario-joiners)
                   (csv/write-csv joiner-scenario-writer (write/joiner-scenario-tap message)))))
      (csv/write-csv joiner-interval-writer (vector write/joiner-interval-headers))
      (add-tap (fn [{:keys [message-type message]}]
                 (when (= message-type :joiner-interval)
                   (csv/write-csv joiner-interval-writer (write/joiner-interval-tap message)))))
      (projection/projection-chan projection-chan
                                  model-seed project-dates
                                  random-seed simulations)
      (loop [completed #{}]
        (when-not (= completed to-complete)
          (recur (conj completed (a/<!! result-chan))))))))

(defn generate-candidates!
  [{{:keys [rewind-years train-years project-years simulations random-seed episodes-extract-date candidate-variations]} :projection-parameters
    file-inputs :file-inputs
    output-parameters :output-parameters
    input-directory :input-directory
    output-directory :output-directory
    config-file :config-file}]
  (when (false? (.exists (io/file output-directory)))
    (.mkdir (java.io.File. output-directory)))
  (let [episodes-extract-date (if (string? episodes-extract-date)
                                (clj-time.format/parse (clj-time.format/formatter "yyyy-MM-dd") episodes-extract-date)
                                episodes-extract-date)
        {:keys [project-from periods]} (prepare-periods (->> file-inputs
                                                             :episodes
                                                             read/episodes
                                                             episodes/remove-f6
                                                             periods/from-episodes
                                                             (hash-map :periods))
                                                        episodes-extract-date rewind-years)
        periods-universe-output (fs/file output-directory "generated-candidates.csv")
        [s1 s2] (rand/split (rand/seed random-seed))
        periods (rand/sample-birthdays periods s1)
        learn-from (time/years-before project-from train-years)
        period-completer (model/markov-placements-model periods (constantly true) learn-from project-from true)
        historic-periods (map #(assoc % :provenance "H" :iteration 0) (remove :open? periods))
        open-periods (map #(assoc % :provenance "O") (filter :open? periods))
        candidate-periods (into [] (comp (map (fn [[iter seed]]
                                                (map (fn [period seed]
                                                       (assoc period
                                                              :iteration iter
                                                              :provenance "C"
                                                              :seed seed))
                                                     (rand/sample-birthdays open-periods seed)
                                                     (rand/split-n seed (count open-periods)))))
                                         cat)
                                (map vector (range candidate-variations) (rand/split-n s2 candidate-variations)))
        _ (println "Finished candidates")
        completed-periods (into [] (comp (map #(assoc % :provenance "P"))
                                         (map period-completer))
                                candidate-periods)
        candidate-periods (map #(update % :seed rand/next-seed) candidate-periods)
        completed-periods (into completed-periods (comp (map #(assoc % :provenance "P" :aged-out? true))
                                                        (map #(period-completer % true)))
                                candidate-periods)
        _ (println "Finished projected periods")
        candidate-periods (map #(update % :seed rand/next-seed) candidate-periods)
        simulated-periods (into [] (comp (map #(assoc % :provenance "S"))
                                         (map (comp period-completer #(periods/period-as-at-wayback % project-from))))
                                candidate-periods)
        candidate-periods (map #(update % :seed rand/next-seed) candidate-periods)
        simulated-periods (into simulated-periods (comp (map #(assoc % :provenance "S" :aged-out? true))
                                                        (map (comp #(period-completer % true) #(periods/period-as-at-wayback % project-from))))
                                candidate-periods)
        _ (println "Finished simulated periods")]
    (->> (concat historic-periods open-periods completed-periods simulated-periods candidate-periods)
         (write/periods-universe)
         (write/write-csv! periods-universe-output))))
