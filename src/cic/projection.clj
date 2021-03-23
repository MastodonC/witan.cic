(ns cic.projection
  (:require [clojure.core.async :as a]
            [cic.model :as model]
            [cic.random :as rand]
            [cic.spec :as spec]
            [cic.summary :as summary]
            [cic.time :as time]
            [kixi.stats.distribution :as d]
            [kixi.stats.protocols :as p]))

(defn project-period-close
  "Sample a possible duration in care which is greater than the existing duration"
  [{:keys [duration-model placements-model] :as model}
   {:keys [duration birthday beginning admission-age episodes period-id open?] :as period} seed]
  #_(if open?
    (let [;; projected-duration (duration-model birthday beginning duration seed)
          {:keys [episodes duration]} (placements-model period seed)]
      (-> (assoc period :duration duration)
          (assoc :episodes episodes)
          (assoc :end (time/without-time (time/days-after beginning duration)))
          (assoc :open? false)))
    period)
  period)

(defn joiners-seq
  "Return a lazy sequence of projected joiners for a particular age of admission."
  [joiners-model duration-model joiner-placements-model markov-model placements-model simulation-model joiner-birthday-model last-joiner previous-offset age end seed]
  (let [[seed-1 seed-2 seed-3 seed-4 seed-5 seed-6] (rand/split-n seed 6)
        interval (joiners-model (time/days-after last-joiner previous-offset) seed-1)
        new-offset (+ previous-offset interval)
        next-time (time/days-after last-joiner new-offset)
        start-time (time/without-time next-time)
        {:keys [episodes-edn admission-age-days duration iterations]} (simulation-model age)
        birthday (time/days-before start-time admission-age-days)
        age-out? (>= (time/year-interval birthday (time/days-after start-time duration)) 17)
        max-duration (time/day-interval start-time (time/day-before-18th-birthday birthday))
        duration (if age-out?
                   max-duration
                   (min duration max-duration))
        period {:beginning start-time
                :birthday birthday
                :episodes (read-string episodes-edn)
                :duration duration
                :end (time/days-after start-time duration)
                :open? false
                :provenance "S"
                :admission-age age
                :dob (time/year birthday)
                :period-id (rand/rand-id 8 seed-4)}]
    (when (time/< next-time end)
      (if period
        (cons period
              (lazy-seq
               (joiners-seq joiners-model duration-model joiner-placements-model markov-model
                            placements-model simulation-model joiner-birthday-model last-joiner new-offset age end seed-5)))
        (lazy-seq
         (joiners-seq joiners-model duration-model joiner-placements-model markov-model
                      placements-model simulation-model joiner-birthday-model last-joiner new-offset age end seed-5))))))

(defn project-joiners
  "Return a lazy sequence of projected joiners for all ages of admission."
  [{:keys [joiners-model joiner-placements-model markov-model duration-model placements-model joiner-birthday-model
           periods project-from
           simulation-model] :as model}
   end seed]
  (let [previous-joiner-per-age (->> (group-by :admission-age periods)
                                     (reduce (fn [m [k v]] (assoc m k (time/max-date (map :beginning v)))) {}))]
    (println "Previous joiner dates per age: " previous-joiner-per-age)
    (mapcat (fn [age seed]
              (let [previous-joiner-at-age (get previous-joiner-per-age age project-from)]
                (joiners-seq (partial joiners-model age project-from)
                             duration-model
                             joiner-placements-model
                             markov-model
                             placements-model
                             simulation-model
                             (partial joiner-birthday-model age)
                             previous-joiner-at-age 0
                             age end seed)))
            spec/ages
            (rand/split-n seed (count spec/ages)))))

(defn init-model
  [{:keys [periods joiner-range episodes-range duration-model joiner-birthday-model project-from project-to segments-range rejection-model] :as model-seed} random-seed]
  (let [[s1 s2] (rand/split-n random-seed 2)]
    model-seed))

(defn train-model
  "Build stochastic helper models using R. Random seed ensures determinism."
  [{:keys [periods joiner-range episodes-range duration-model joiner-birthday-model project-to project-from segments-range markov-model
           projection-model simulation-model age-out-model
           age-out-projection-model age-out-simulation-model] :as model-seed} random-seed]
  (println "Training model...")
  (let [[s1 s2 s3] (rand/split-n random-seed 3)
        [joiners-from joiners-to] joiner-range
        [episodes-from episodes-to] episodes-range
        [learn-from learn-to] segments-range
        all-periods (rand/sample-birthdays periods s1)
        closed-periods (rand/close-open-periods all-periods projection-model age-out-model age-out-projection-model s3)]
    {:joiners-model (-> (filter #(time/between? (:beginning %) joiners-from joiners-to) all-periods)
                        (model/joiners-model-gen project-to s2))
     :joiner-birthday-model joiner-birthday-model
     :placements-model (model/periods->placements-model closed-periods episodes-from episodes-to)
     :markov-model markov-model
     :joiner-placements-model (model/joiner-placements-model all-periods)
     :periods closed-periods
     :project-from project-from
     :projection-model projection-model
     :simulation-model (fn [admission-age]
                         (or (when (or (age-out-model admission-age nil) (= admission-age 17)) ;; TODO Passing nil because seed isn't used yet
                               (age-out-simulation-model admission-age))
                             (simulation-model admission-age)))}))

(defn project-1
  "Returns a single sequence of projected periods."
  [model-seed end seed]
  (let [[s1 s2 s3 s4] (rand/split-n seed 4)
        model (train-model model-seed s1)]
    (concat (:periods model) (project-joiners model end s3))))

(defn projection-chan
  [model-seed project-dates seed n-runs]
  (let [max-date (time/max-date project-dates)
        [s1 s2] (rand/split-n (rand/seed seed) 2)
        model-seed (init-model model-seed s1)
        parallelism (* 3 (quot (.availableProcessors (Runtime/getRuntime)) 4)) ;; use 3/4 the cores
        in-chan (a/to-chan! (map-indexed vector (rand/split-n s2 n-runs)))
        out-chan (a/chan 1024)
        simulation-number (atom 0)
        projection-xf (map (fn [[iteration seed]]
                             (into []
                                   (map #(assoc % :simulation-number (inc iteration)))
                                   (project-1 model-seed max-date seed))))
        _ (a/pipeline-blocking parallelism out-chan projection-xf in-chan)]
    out-chan))

(defn projection
  [model-seed project-dates placement-costs seed n-runs]
  (let [out-chan (a/chan 1024 (map #(summary/periods-summary % project-dates placement-costs)))
        _ (a/pipe (projection-chan model-seed project-dates seed n-runs)
                  out-chan)
        ]
    (summary/grand-summary
     (a/<!!
      (a/into [] out-chan)))))

(defn project-n
  [model-seed project-dates seed n-runs]
  (let [out-chan (projection-chan model-seed project-dates seed n-runs)]
    (a/<!! (a/into [] out-chan))))

(defn cost-projection
  [projection-seed model-seed project-until placement-costs seed n-runs]
  (->> (project-n projection-seed model-seed [project-until] seed n-runs)
       (summary/annual placement-costs)))
