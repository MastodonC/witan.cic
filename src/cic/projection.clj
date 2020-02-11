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
  [{:keys [duration-model episodes-model placements-model] :as model}
   {:keys [duration birthday beginning admission-age episodes period-id] :as open-period} seed]
  (let [projected-duration (duration-model birthday beginning duration seed)
        episodes (placements-model admission-age projected-duration open-period seed)]
    (-> (assoc open-period :duration projected-duration)
        (assoc :episodes episodes)
        (assoc :end (time/without-time (time/days-after beginning projected-duration)))
        (assoc :open? false))))

(defn joiners-seq
  "Return a lazy sequence of projected joiners for a particular age of admission."
  [joiners-model duration-model episodes-model age beginning end seed]
  (let [[seed-1 seed-2 seed-3 seed-4 seed-5 seed-6] (rand/split-n seed 6)
        interval (joiners-model beginning seed-1)
        next-time (time/days-after beginning interval)
        start-time (time/without-time next-time)
        birthday (-> (time/days-before start-time (p/sample-1 (d/uniform {:a 0 :b 364}) seed-6))
                     (time/years-before age))
        duration (duration-model birthday start-time seed-2)
        episodes (episodes-model duration seed-3)]
    (when (time/< next-time end)
      (let [period-end (time/days-after next-time duration)
            period {:beginning start-time
                    :end (time/without-time period-end)
                    :period-id (rand/rand-id 8 seed-4)
                    :birthday birthday
                    :admission-age age
                    :dob (time/year birthday)
                    :episodes episodes}]
        (cons period
              (lazy-seq
               (joiners-seq joiners-model duration-model episodes-model age next-time end seed-5)))))))

(defn project-joiners
  "Return a lazy sequence of projected joiners for all ages of admission."
  [{:keys [joiners-model duration-model episodes-model] :as model} beginning end seed]
  (mapcat (fn [age seed]
            (joiners-seq (partial joiners-model age)
                         duration-model
                         (partial episodes-model age) age beginning end seed))
          spec/ages
          (rand/split-n seed (count spec/ages))))

(defn train-model
  "Build stochastic helper models using R. Random seed ensures determinism."
  [{:keys [seed joiner-range episodes-range duration-model placements-model] :as model-seed} random-seed]
  (let [[s1 s2] (rand/split-n random-seed 2)
        [joiners-from joiners-to] joiner-range
        [episodes-from episodes-to] episodes-range
        periods (rand/prepare-ages seed s1)
        closed-periods (filter :end periods)]
    {:joiners-model (-> (filter #(time/between? (:beginning %) joiners-from joiners-to) periods)
                        (model/joiners-model-gen s2))
     :episodes-model (-> (filter #(time/between? (:end %) episodes-from episodes-to) closed-periods)
                         (model/episodes-model))
     :duration-model duration-model
     :placements-model placements-model}))

(defn project-1
  "Returns a single sequence of projected periods."
  [projection-seed model-seed end seed]
  (let [[s1 s2 s3 s4] (rand/split-n seed 4)
        model (train-model model-seed s1)
        projection-seed (update projection-seed :seed rand/prepare-ages s4)]
    (-> (map (partial project-period-close model) (:seed projection-seed) (rand/split-n s2 (count (:seed projection-seed))))
        (concat (project-joiners model (:date projection-seed) end s3)))))

(defn project-n
  "Returns n stochastic sequences of projected periods."
  [projection-seed model-seed project-dates seed n-runs]
  (let [max-date (time/max-date project-dates)]
    (map #(project-1 projection-seed model-seed max-date %)
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
