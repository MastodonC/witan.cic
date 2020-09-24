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
  [joiners-model duration-model placements-model joiner-birthday-model last-joiner previous-offset age end seed]
  (let [[seed-1 seed-2 seed-3 seed-4 seed-5 seed-6] (rand/split-n seed 6)
        interval (joiners-model (time/days-after last-joiner previous-offset) seed-1)
        new-offset (+ previous-offset interval)
        next-time (time/days-after last-joiner new-offset)
        start-time (time/without-time next-time)
        birthday (joiner-birthday-model start-time seed-6)
        ;; duration (duration-model birthday start-time seed-2)
        {:keys [duration episodes]} (placements-model {:beginning start-time :birthday birthday} seed-3)]
    (when (time/< next-time end)
      (let [period-end (time/without-time (time/days-after start-time duration))
            period {:beginning start-time
                    :end period-end
                    :period-id (rand/rand-id 8 seed-4)
                    :birthday birthday
                    :admission-age age
                    :dob (time/year birthday)
                    :episodes episodes
                    :provenance "S"}]
        (cons period
              (lazy-seq
               (joiners-seq joiners-model duration-model placements-model joiner-birthday-model last-joiner new-offset age end seed-5)))))))

(defn project-joiners
  "Return a lazy sequence of projected joiners for all ages of admission."
  [{:keys [joiners-model duration-model placements-model joiner-birthday-model periods project-from] :as model}
   end seed]
  (let [previous-joiner-per-age (->> (group-by :admission-age periods)
                                     (reduce (fn [m [k v]] (assoc m k (time/max-date (map :beginning v)))) {}))]
    (mapcat (fn [age seed]
              (let [previous-joiner-at-age (get previous-joiner-per-age age project-from)]
                (println (str "Previous joiner at age " age " " previous-joiner-at-age " " project-from))
                (joiners-seq (partial joiners-model age project-from)
                             duration-model
                             placements-model
                             (partial joiner-birthday-model age)
                             previous-joiner-at-age 0
                             age end seed)))
            spec/ages
            (rand/split-n seed (count spec/ages)))))

(defn init-model
  [{:keys [periods joiner-range episodes-range duration-model joiner-birthday-model project-from project-to] :as model-seed} random-seed]
  (let [[s1 s2] (rand/split-n random-seed 2)
        all-periods (rand/sample-birthdays periods s1)
        knn-closed-cases (model/knn-closed-cases all-periods project-from s2)]
    (assoc model-seed :knn-closed-cases knn-closed-cases)))

(defn train-model
  "Build stochastic helper models using R. Random seed ensures determinism."
  [{:keys [periods joiner-range episodes-range duration-model joiner-birthday-model project-to knn-closed-cases] :as model-seed} random-seed]
  (let [[s1 s2 s3] (rand/split-n random-seed 3)
        [joiners-from joiners-to] joiner-range
        [episodes-from episodes-to] episodes-range
        all-periods (rand/sample-birthdays periods s1)
        closed-periods (rand/close-open-periods all-periods knn-closed-cases s3)]
    {:joiners-model (-> (filter #(time/between? (:beginning %) joiners-from joiners-to) closed-periods)
                        (model/joiners-model-gen project-to s2))
     :joiner-birthday-model joiner-birthday-model
     :placements-model (model/periods->placements-model closed-periods episodes-from episodes-to)
     :periods closed-periods}))

(defn project-1
  "Returns a single sequence of projected periods."
  [model-seed end seed]
  (let [[s1 s2 s3 s4] (rand/split-n seed 4)
        model (train-model model-seed s1)
        model (update model :periods rand/sample-birthdays s4)]
    (concat (:periods model) (project-joiners model end s3))))

(defn project-n
  "Returns n stochastic sequences of projected periods."
  [model-seed project-dates seed n-runs]
  (let [max-date (time/max-date project-dates)
        [s1 s2] (rand/split-n (rand/seed seed) 2)
        model-seed (init-model model-seed s1)]
    (map-indexed (fn [iteration seed]
                   (->> (project-1 model-seed max-date seed)
                        (map #(assoc % :simulation-number (inc iteration)))))
                 (rand/split-n s2 n-runs))))

(defn projection
  "Calculates summary statistics over n sequences of projected periods."
  [model-seed project-dates placement-costs seed n-runs]
  (->> (project-n model-seed project-dates seed n-runs)
       (map #(summary/periods-summary % project-dates placement-costs))
       (summary/grand-summary)))

(defn cost-projection
  [projection-seed model-seed project-until placement-costs seed n-runs]
  (->> (project-n projection-seed model-seed [project-until] seed n-runs)
       (summary/annual placement-costs)))
