(ns cic.projection
  (:require [cic.model :as model]
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
  (if open?
    (let [projected-duration (duration-model birthday beginning duration seed)
          episodes (placements-model admission-age projected-duration period seed)]
      (-> (assoc period :duration projected-duration)
          (assoc :episodes episodes)
          (assoc :end (time/without-time (time/days-after beginning projected-duration)))
          (assoc :open? false)))
    period))

(defn joiners-seq
  "Return a lazy sequence of projected joiners for a particular age of admission."
  [joiners-model duration-model placements-model joiner-birthday-model last-joiner previous-offset age end seed]
  (let [[seed-1 seed-2 seed-3 seed-4 seed-5 seed-6] (rand/split-n seed 6)
        interval (joiners-model (time/days-after last-joiner previous-offset) seed-1)
        new-offset (+ previous-offset interval)
        next-time (time/days-after last-joiner new-offset)
        start-time (time/without-time next-time)
        birthday (joiner-birthday-model start-time seed-6)
        duration (duration-model birthday start-time seed-2)
        episodes (placements-model duration {:beginning start-time :birthday birthday
                                             :duration 0 :episodes []} seed-3)]
    (when (time/< next-time end)
      (let [period-end (time/without-time (time/days-after start-time duration))
            period {:beginning start-time
                    :end period-end
                    :period-id (rand/rand-id 8 seed-4)
                    :birthday birthday
                    :admission-age age
                    :dob (time/year birthday)
                    :episodes episodes}]
        (cons period
              (lazy-seq
               (joiners-seq joiners-model duration-model placements-model joiner-birthday-model last-joiner new-offset age end seed-5)))))))

(defn project-joiners
  "Return a lazy sequence of projected joiners for all ages of admission."
  [{:keys [joiners-model duration-model placements-model joiner-birthday-model] :as model}
   projection-seed end seed]
  (let [previous-joiner-per-age (->> (group-by :admission-age (:seed projection-seed))
                                     (reduce (fn [m [k v]] (assoc m k (time/max-date (map :beginning v)))) {}))]
    (mapcat (fn [age seed]
              (let [previous-joiner-at-age (get previous-joiner-per-age age (:date projection-seed))]
                (joiners-seq (partial joiners-model age (:date projection-seed))
                             duration-model
                             (partial placements-model age)
                             (partial joiner-birthday-model age)
                             previous-joiner-at-age 0
                             age end seed)))
            spec/ages
            (rand/split-n seed (count spec/ages)))))

(defn train-model
  "Build stochastic helper models using R. Random seed ensures determinism."
  [{:keys [seed joiner-range episodes-range duration-model joiner-birthday-model phase-durations project-to] :as model-seed} random-seed]
  (let [[s1 s2] (rand/split-n random-seed 2)
        [joiners-from joiners-to] joiner-range
        [episodes-from episodes-to] episodes-range
        periods (rand/sample-birthdays seed s1)
        closed-periods (filter :end periods)]
    {:joiners-model (-> (filter #(time/between? (:beginning %) joiners-from joiners-to) periods)
                        (model/joiners-model-gen project-to s2))
     :placements-model (model/periods->placements-model periods episodes-from episodes-to)
     :phase-durations phase-durations
     :joiner-birthday-model joiner-birthday-model
     :duration-model duration-model
     :periods periods}))

(defn project-1
  "Returns a single sequence of projected periods."
  [projection-seed model-seed end seed]
  (let [[s1 s2 s3 s4] (rand/split-n seed 4)
        model (train-model model-seed s1)
        projection-seed (update projection-seed :seed rand/sample-birthdays s4)]
    (-> (map (partial project-period-close model) (:seed projection-seed) (rand/split-n s2 (count (:seed projection-seed))))
        #_(concat (project-joiners model projection-seed end s3)))))

(defn project-n
  "Returns n stochastic sequences of projected periods."
  [projection-seed model-seed project-dates seed n-runs]
  (let [max-date (time/max-date project-dates)]
    (map-indexed (fn [iteration seed]
                   (->> (project-1 projection-seed model-seed max-date seed)
                        (map #(assoc % :simulation-number (inc iteration)))))
                 (-> (rand/seed seed)
                     (rand/split-n n-runs)))))

(defn projection
  "Calculates summary statistics over n sequences of projected periods."
  [projection-seed model-seed project-dates placement-costs seed n-runs]
  (->> (project-n projection-seed model-seed project-dates seed n-runs)
       (map #(summary/periods-summary % project-dates placement-costs))
       (summary/grand-summary)))

(defn cost-projection
  [projection-seed model-seed project-until placement-costs seed n-runs]
  (->> (project-n projection-seed model-seed [project-until] seed n-runs)
       (summary/annual placement-costs)))
