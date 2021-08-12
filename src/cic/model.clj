(ns cic.model
  (:require [cic.episodes :as episodes]
            [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.periods :as periods]
            [cic.random :as rand]
            [cic.rscript :as rscript]
            [cic.spec :as spec]
            [cic.time :as time]
            [clj-time.core :as t]
            [clojure.math.combinatorics :as c]
            [clojure.set :as set]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]
            [kixi.stats.math :as m]
            [kixi.stats.protocols :as p]
            [taoensso.timbre :as log]))

(def period-in-days
  84)

(def min-exponential-rate
  (/ 1.0 365.25))

(def month->day-factor
  (/ 12 365.25))

(def month->period-factor
  (* month->day-factor period-in-days))

(def period->month-factor
  (/ 1 month->period-factor))

(def period->day-factor
  (/ 1.0 period-in-days))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [{:keys [model-coefs]} project-from project-to simulation-id seed]
  ;; Work out a sample rate for every future day
  (let [ ;; Our current modelling is based on a joiners per 84 days
        periods (time/day-seq project-from project-to period-in-days)
        seeds (rand/split-n seed (count periods))
        min-period (first periods)
        max-period (time/days-before (last periods) 1)
        day-rate-rows (for [[[period-from period-to] seed] (map vector (partition 2 1 periods) seeds)
                            age spec/ages]
                        (let [day (t/in-days (t/interval (t/epoch) (time/halfway-between period-from period-to)))
                              intercept (get model-coefs "(Intercept)")
                              a (get model-coefs (str "admission_age" age) 0.0)
                              b (get model-coefs "quarter")
                              c (get model-coefs (str "quarter:admission_age" age) 0.0)
                              y (m/exp (+ intercept a (* b day) (* c day)))
                              n-per-day (* y period->day-factor) ;; The R code assumes a quarter is 3 * 28 days
                              ]
                          {:period-from period-from
                           :period-to period-to
                           :age age
                           :n-per-day n-per-day
                           :n-per-period 0
                           :y-per-period y
                           :simulation-id simulation-id}))
        
        ;; _ (write/write-csv! "joiner-rates.csv" (write/joiner-rates day-rate-rows))
        day-rate-lookup (->> (mapcat (fn [{:keys [period-from period-to age n-per-day]}]
                                       (map (fn [day]
                                              {:age age :day day :n-per-day n-per-day})
                                            (time/day-seq period-from period-to)))
                                     day-rate-rows)
                             (reduce (fn [coll {:keys [age day n-per-day]}]
                                       (assoc coll [age day] n-per-day))
                                     {}))]
    (tap> {:message-type :joiners
           :message day-rate-rows})
    (fn [age join-after previous-joiner seed]
      (loop [seed seed sample-adjustment 0]
        (let [n-per-day (or (get day-rate-lookup [age previous-joiner])
                            (when (time/< previous-joiner min-period)
                              (get day-rate-lookup [age min-period]))
                            (get day-rate-lookup [age max-period]))
              exponential-rate (max n-per-day min-exponential-rate)
              sample (+ sample-adjustment (p/sample-1 (d/exponential {:rate exponential-rate}) seed))
              join-date (time/days-after previous-joiner sample)]
          (if (time/>= join-date join-after)
            (do (tap> {:message-type :joiner-interval :message [{:simulation-id simulation-id :age age :join-date join-date :n-per-day n-per-day :target-rate exponential-rate :interval-days sample :interval-adjustment sample-adjustment}]})
                sample)
            (recur (rand/next-seed seed) (inc sample-adjustment))))))))

(defn linear-interpolation
  [a b n]
  (let [d (/ (- b a) n)]
    (take n (iterate (partial + d) a))))

(defn poisson-crosscheck
  []
  (let [lambdas [0.5 1 1.5 2 2.5 3 3.5 4 4.5 5]
        samples (mapcat (fn [lambda]
                          (let [dist (d/poisson {:lambda lambda})
                                samples (d/sample 10000 dist)]
                            (map #(hash-map :lambda lambda :sample %) samples)))
                        lambdas)]
    (write/mapseq->csv! samples)))

(defn exponential-crosscheck
  []
  (let [lambdas [0.5 1 1.5 2 2.5 3 3.5 4 4.5 5]
        samples (mapcat (fn [lambda]
                          (let [dist (d/exponential {:rate lambda})
                                samples (d/sample 10000 dist)]
                            (map #(hash-map :lambda lambda :sample %) samples)))
                        lambdas)]
    (write/mapseq->csv! samples)))

(defn poisson-exponential-crosscheck
  []
  (let [lambdas [0.5 1 1.5 2 2.5 3 3.5 4 4.5 5]
        samples (mapcat (fn [lambda]
                          (let [dist (d/poisson {:lambda lambda})
                                samples (d/sample 10000 dist)]
                            (map (fn [sample-1]
                                   (let [constrained-sample (max sample-1 min-exponential-rate)
                                         dist (d/exponential {:rate constrained-sample})
                                         sample-2 (d/draw dist)]
                                     (hash-map :lambda lambda :constrained-sample constrained-sample :poisson-sample sample-1 :exponential-sample sample-2))) samples)))
                        lambdas)]
    (write/mapseq->csv! samples)))

(defn scenario-joiners-model
  [joiner-rates project-from project-to simulation-id seed]
  (log/info "scenario-joiners-model begin")
  (let [scenario-dates (sort (map :date joiner-rates))
        rates-by-age (reduce (fn [coll x]
                               (assoc coll x
                                      (map (juxt :date (keyword (str x))) joiner-rates))) {}
                             (range 18))
        day-rates (into []
                        (mapcat (fn [age]
                                  (let [rates (let [[interpolation-start-date rate] (first (get rates-by-age age))]
                                                (into []
                                                      (map (fn [date]
                                                             {:age age :day date :n-per-month rate :simulation-id simulation-id}))
                                                      (time/day-seq project-from interpolation-start-date)))
                                        rates (let [[interpolation-end-date rate] (last (get rates-by-age age))]
                                                (into rates
                                                      (map (fn [date]
                                                             {:age age :day date :n-per-month rate :simulation-id simulation-id}))
                                                      (time/day-seq interpolation-end-date (time/days-after project-to 2))))
                                        rates (into rates
                                                    (mapcat
                                                     (fn [[[d1 r1] [d2 r2]]]
                                                       (let [dates (time/day-seq d1 d2)
                                                             rates (linear-interpolation r1 r2 (count dates))]
                                                         (for [[date rate] (map vector dates rates)]
                                                           {:age age :day date :n-per-month rate :simulation-id simulation-id}))))
                                                    (partition 2 1 (get rates-by-age age)))
                                        period-rates (partition-all period-in-days rates)]
                                    (sequence (mapcat (fn [[period-rates seed]]
                                                        (let [period-rate (* month->period-factor (transduce (map :n-per-month) k/mean period-rates))
                                                              n-per-period period-rate #_(p/sample-1 (d/poisson {:lambda period-rate}) seed)
                                                              n-per-day (* period->day-factor n-per-period)]
                                                          (into []
                                                                (map (fn [rate]
                                                                       (assoc rate :n-per-day n-per-day)))
                                                                period-rates))))
                                              (map vector period-rates (rand/split-n seed (count period-rates)))))))
                        (range 18))
        day-rate-lookup (reduce (fn [coll {:keys [age day n-per-day]}]
                                  (assoc coll [age day] n-per-day))
                                {}
                                day-rates)]
    (tap> {:message-type :scenario-joiners :message day-rates})
    ;; (log/info "scenario-joiners-model end")
    (fn [age join-after previous-joiner seed]
      (loop [seed seed sample-adjustment 0]
        (let [n-per-day (or (get day-rate-lookup [age previous-joiner])
                            (when (time/< previous-joiner project-from)
                              (get day-rate-lookup [age project-from]))
                            (get day-rate-lookup [age project-to]))
              target-rate (max n-per-day min-exponential-rate)
              sample (p/sample-1 (d/exponential {:rate target-rate}) seed)
              ;; _ (log/info "age" age "n-per-day" n-per-day "interarrival" sample)
              ;;_ (log/info sample)
              sample (+ sample-adjustment sample)
              join-date (time/days-after previous-joiner sample)]
          (if (time/>= join-date join-after)
            (do (tap> {:message-type :joiner-interval :message [{:simulation-id simulation-id :age age :join-date join-date :n-per-day n-per-day :target-rate target-rate :interval-days sample :interval-adjustment sample-adjustment}]})
                sample)
            (do (log/info "looping..." age join-date join-after sample-adjustment)
                (recur (rand/next-seed seed) (inc sample-adjustment)))))))))

(defn joiners-model-gen
  "Wraps R to trend joiner rates into the future."
  [periods project-from project-to joiner-model-type scenario-joiner-rates simulation-id seed]
  (cond
    (#{:trended :untrended} joiner-model-type)
    (let [script "src/joiners.R"
          input (str (rscript/write-periods! periods))
          output (str (write/temp-file "file" ".csv"))
          [s1 s2] (rand/split seed)
          seed-long (rand/rand-long s1)
          trend-joiners? (= joiner-model-type :trended)]
      (rscript/exec script input output
                    (time/date-as-string project-to)
                    (str trend-joiners?)
                    (str (Math/abs seed-long)))
      (-> (read/joiner-csv output)
          (joiners-model project-from project-to simulation-id s2)))
    (= joiner-model-type :scenario)
    (scenario-joiners-model scenario-joiner-rates project-from project-to simulation-id seed)))

(defn sample-ci
  "Given a 95% lower bound, median and 95% upper bound,
  sample from a skewed normal with these properties.
  We make use of the fact that the normal 95% CI is +/- 1.96"
  [lower median upper seed]
  (let [normal (p/sample-1 (d/normal {:mu 0 :sd 1}) seed)]
    (if (pos? normal)
      (+ median (* (- upper median) (/ normal 1.96)))
      (- median (* (- median lower) (/ normal -1.96))))))

(defn clamp
  [lower x upper]
  (max (min x upper) lower))

(defn duration-model
  "Given an admitted date and age of a child in care,
  returns an expected duration in days"
  [coefs]
  (fn duration-model*
    ([birthday beginning seed]
     (duration-model* birthday beginning 0 seed))
    ([birthday beginning min-value seed]
     (let [age (time/year-interval birthday beginning)
           empirical (get coefs (max 0 (min age 17)))
           n (transduce (take-while (fn [[_ m _]] (<= m min-value))) k/count empirical)
           [r1 r2] (rand/split seed)
           max-value (dec (time/day-interval birthday (time/years-after birthday 18)))]
       (if (> n 100)
         (int (p/sample-1 (d/uniform {:a min-value :b max-value}) r1))
         (loop [r1 r1 iter 1]
           (let [quantile (int (p/sample-1 (d/uniform {:a n :b 100}) r1))
                 [lower median upper] (get empirical quantile)
                 sample (sample-ci lower median upper r2)]
             (if (and (< sample min-value) (< iter 5))
               (recur (second (rand/split r1)) (inc iter))
               (clamp min-value sample max-value)))))))))

(defn cease-model
  [coefs]
  (fn cease-model*
    [birthday beginning elapsed-duration seed]
    (let [age-entry (max 0 (min (time/year-interval birthday beginning) 17))
          hazard (or (some (fn [{:keys [duration hazard]}]
                             (when (>= duration elapsed-duration)
                               hazard))
                           (get coefs age-entry []))
                     1.0)]
      (p/sample-1 (d/bernoulli {:p hazard}) seed))))

(defn update-fuzzy
  "Like `update`, but the key is expected to be a vector of values.
  Any numeric values in the key are fuzzed, so for example
  `(update-fuzzy {} [5 0.5] conj :value)` will return:
  {(4 0) (:value),
   (4 1) (:value),
   (5 0) (:value),
   (5 1) (:value),
   (6 0) (:value),
   (6 1) (:value)}
  This enables efficient lookup of values which are similar to,
  but not neccessarily identical to, the input key."
  [coll ks f & args]
  (let [ks (mapv (fn [k]
                   (cond
                     (or (double? k) (ratio? k))
                     [(int (m/floor k)) (int (m/ceil k))]
                     (int? k)
                     [(dec k) k (inc k)]
                     :else [k]))
                 ks)]
    (reduce (fn [coll ks]
              (apply update coll ks f args))
            coll
            (apply c/cartesian-product ks))))

(defn period->phases
  [{:keys [birthday beginning end episodes] :as period} episodes-from episodes-to]
  (for [[{offset-a :offset from :placement} {offset-b :offset to :placement}] (partition-all 2 1 episodes)
        ;; FIXME: model needs a default for when there aren't enough recent records
        ;; :when (time/between? (time/days-after beginning offset-a) episodes-from episodes-to)
        ]
    (let [total-duration (time/day-interval beginning end)]
      {:total-duration total-duration
       :phase-duration (if offset-b
                         (- offset-b offset-a)
                         (- (time/day-interval beginning end) offset-a))
       :first-phase (zero? offset-a)
       :age (time/year-interval birthday (time/days-after beginning offset-a))})))

(defn phase-durations
  "Calculate the phase durations for all closed periods"
  [periods episodes-from episodes-to]
  (let [phases (into []
                     (comp (remove :open?)
                           (mapcat #(period->phases % episodes-from episodes-to)))
                     periods)
        input (str (rscript/write-phase-durations! phases))
        phase-duration-quantiles-out (str (write/temp-file "phase-duration-quantiles" ".csv"))
        phase-beta-params-out (str (write/temp-file "phase-beta-params" ".csv"))
        script "src/phase-durations.R"]
    (rscript/exec script input
                  phase-duration-quantiles-out
                  phase-beta-params-out)
    {:phase-duration-quantiles (read/phase-duration-quantiles-csv phase-duration-quantiles-out)
     :phase-beta-params (read/age-beta-params phase-beta-params-out)}))

(defn filter-transitions
  [transitions min-duration]
  (reduce (fn [acc [label opts]]
            (reduce (fn [acc [key opts]]
                      (reduce (fn [acc [to {:keys [n durations]}]]
                                (let [durations (filter #(> % min-duration) durations)]
                                  (if (seq durations)
                                    (-> (assoc-in acc [label key to :n] (count durations))
                                        (assoc-in [label key to :durations] durations))
                                    acc)))
                              acc
                              opts))
                    acc
                    opts))
          {}
          transitions))

(defn dirichlet-categorical
  [category-alphas]
  (let [[categories alphas] (apply map vector category-alphas)]
    (d/draw (d/categorical (zipmap categories (d/draw (d/dirichlet {:alphas alphas})))))))

(defn periods->placements-model
  [periods episodes-from episodes-to]
  (println "Deprecated")
  #_(let [age-groups (group-by :admission-age periods)]
    (fn [{:keys [beginning birthday]} seed]
      (let [age (min (time/year-interval birthday beginning) 17)
            period (-> (get age-groups age)
                       (rand-nth)
                       (select-keys [:duration :episodes]))]
        (assert (:duration period) (format "Period %s has no duration, %s %s" period beginning birthday))
        period))))

(defn joiner-birthday-model
  "Accepts quantiles for age zero joiner ages in days and returns a birthday-generating model
  FIXME: Create a DSDR documenting the fact that age zero joiners have been observed to join
  soon after birth. This means that age zero joiners tend disproportionately to be only days
  old when joining. Without adjusting for this we will generate too many older joiners
  which will manifest itself as an increase in age 1 year CiC, and so on. Those children
  previously would have left the system before their first birthday."
  [quantiles]
  (let [q (vec quantiles)
        n (count quantiles)
        dist (d/uniform {:a 0 :b n})]
    (fn [age join-date seed]
      (if (zero? age)
        (let [i (int (p/sample-1 dist seed))]
          (time/days-before join-date (get q i)))
        (-> (time/days-before join-date (int (p/sample-1 (d/uniform {:a 0 :b 364}) seed)))
            (time/years-before age))))))

(defn knn-closed-cases
  [periods project-from seed]
  (let [clusters-out (str (write/temp-file "file" ".csv"))
        periods-in (write/periods->knn-closed-cases-csv periods)
        script "src/close-open-cases.R"
        algo "euclidean_scaled"
        tiers 1
        seed-long (rand/rand-long seed)]
    (rscript/exec script periods-in clusters-out (time/date-as-string project-from) algo (str tiers) (str (Math/abs ^long seed-long)))
    (read/knn-closed-cases clusters-out)))


(def offset-groups-filtered* (atom nil))
(def offset-groups-all* (atom nil))
(def placement-groups* (atom nil))
(def matched-segments* (atom nil))

(defn offset-groups
  [periods learn-from learn-to close-open-periods?]
  (let [id-seq (atom 0)
        segments (into []
                       (comp (mapcat periods/segment)
                             (map #(assoc % :in-filter? (and (time/>= (:date %) learn-from)
                                                             (time/<= (:date %) learn-to)))))
                       periods)
        offsets (if close-open-periods?
                  (range periods/segment-interval)
                  [0])
        segments (sequence
                  (mapcat (fn [offset]
                            (into []
                                  (comp (map #(periods/tail-segment % offset))
                                        (map #(assoc % :offset offset)))
                                  segments)))
                  offsets)]
    (merge
     (group-by (juxt :offset :from-placement :terminal?)
               segments)
     (group-by (juxt :offset :from-placement)
               segments)
     (group-by (juxt :from-placement)
               segments))))

(defn min-key'
  "Like clojure.core/min-key but expects a sequence of xs
  and returns nil for an empty sequence"
  [f xs]
  (when (seq xs)
    (apply min-key f xs)))

(defn distance
  "Returns the absolute distance between x and y"
  [^long x ^long y]
  (Math/abs (- x y)))

(defn euclidean-distance
  [as bs]
  (->> (map (fn [^long a ^long b]
              (Math/pow (- a b) 2))
            as bs)
       (reduce +)
       (Math/sqrt)))

(defn jitter-normal
  [sd]
  (let [dist (d/normal {:mu 0 :sd sd})]
    (fn [x]
      (+ x (d/draw dist)))))

(defn jitter-binomial
  [x n scale seed]
  (let [p (/ x n)
        dist (d/binomial {:n (/ n scale) :p p})]
    (long (* scale (p/sample-1 dist seed)))))

(defn get-matched-segment
  [feature-fn feature-vec segments]
  (min-key'
   (fn [segment]
     (euclidean-distance (feature-fn segment) feature-vec))
   segments))

(def max-age-days (* 365 18))

(def jitter-scale 7) ;; Higher is more jittering


(defn admission-age-care-weeks-pdf
  [sample]
  (let [inc' (fnil inc 0)
        +' (fnil + 0)
        counts (reduce (fn [acc {:keys [admission-age care-weeks] :as simulation}]
                         (-> acc
                             (update-in [admission-age care-weeks] inc')
                             (update-in [admission-age :n] inc')))
                       {}
                       sample)]
    (reduce (fn [counts age]
              (let [n (get-in counts [age :n] 0)
                    
                    adj (/ 1 52)]
                (reduce (fn [counts week]
                          (-> counts
                              (update-in [age week] +' adj)
                              (update-in [age week] / (inc n))
                              (update-in [age week] double)))
                        (update counts age dissoc :n)
                        (range 52))))
            counts
            (range 18))))

(defn accept?
  [proposal desired]
  (and proposal
       desired
       (let [dist (d/uniform {:a 0 :b proposal})]
         (<= (d/draw dist) desired))))

(defn markov-period
  [{:keys [episodes birthday beginning duration period-id provenance seed] :as period} offset-segments rejection-model & [age-out?]]
  (assert provenance)
  (let [admission-age (time/year-interval birthday beginning)
        max-duration (dec (time/day-interval beginning (time/years-after birthday 18)))
        init-duration duration
        init-placement (-> episodes last :placement)
        init-episodes episodes
        init-offset (rem duration periods/segment-interval)
        init-initial? (< duration periods/segment-interval)
        [episodes total-duration iterations next-seed]
        (loop [total-duration init-duration
               last-placement init-placement
               all-episodes init-episodes
               offset init-offset
               initial? init-initial?
               counter 0
               seed seed]
          (let [[s1 s2 s3] (rand/split-n seed 3)
                age-days (time/day-interval birthday (time/days-after beginning total-duration))
                age-days (jitter-binomial age-days max-age-days jitter-scale s1)
                join-age-days (time/day-interval birthday beginning)
                join-age-days (jitter-binomial join-age-days max-age-days jitter-scale s2)
                ;; care-days (jitter-binomial total-duration max-duration jitter-scale)
                care-days total-duration
                {:keys [terminal? episodes duration to-placement aged-out?] :as sample}
                (get-matched-segment (juxt :join-age-days :care-days) [join-age-days care-days]
                                     (or (when age-out?
                                           ;; Will the period we get from this match take us to within 3 months of 18th birthday?
                                           (if (>= (- periods/segment-interval offset)
                                                   (- max-duration total-duration period-in-days))
                                             ;; If yes, make it a terminal segment. Else ensure it's not.
                                             (get offset-segments [offset last-placement true])
                                             (get offset-segments [offset last-placement false])))
                                         (get offset-segments [offset last-placement])
                                         (get offset-segments [last-placement])
                                         (do (println "No sample found - ignoring")
                                             nil)))]
            #_(when-not sample (println (format "No sample for age %s, placement %s for offset %s initial %s even outside filter" age-days last-placement offset initial?)))
            (when-not sample
              [nil nil])
            (let [episodes (concat all-episodes (episodes/add-offset total-duration episodes))
                  total-duration' (if aged-out?
                                    max-duration
                                    (min (+ total-duration duration) max-duration))
                  care-weeks (quot total-duration' 7)
                  iterations-exceeded? (>= counter 1000)]
              (cond
                ;; Resample 90% of terminal cases
                ;; (and terminal? (< total-duration' max-duration) (> (rand) 0.1))
                ;; (recur total-duration last-placement all-episodes offset initial?)

                (or terminal? (>= total-duration' max-duration))
                [(take-while #(< (:offset %) total-duration') episodes)
                 (jitter-binomial total-duration' max-duration jitter-scale s3)
                 counter
                 (rand/next-seed seed)]

                :else
                (recur total-duration'
                       to-placement
                       episodes
                       0               ;; Zero offset
                       (boolean false) ;; Always return false
                       (inc counter)
                       (rand/next-seed seed))))))]
    (when (and episodes total-duration)
      (-> period
          (assoc :seed next-seed)
          (assoc :episodes (episodes/simplify episodes))
          (assoc :duration total-duration)
          (assoc :open? false)
          (assoc :end (time/days-after beginning total-duration))
          (assoc :iterations iterations)))))

(defn markov-placements-model
  [periods rejection-model learn-from learn-to close-open-periods?]
  (let [offset-segments (offset-groups periods learn-from learn-to close-open-periods?)]
    (fn
      ([{:keys [episodes birthday beginning duration period-id] :as period}]
       (markov-period period offset-segments rejection-model))
      ([{:keys [episodes birthday beginning duration period-id] :as period} age-out?]
       (markov-period period offset-segments rejection-model age-out?)))))

(defn joiner-placements-model
  [periods]
  (println "Deprecated")
  #_(let [coefs (reduce (fn [acc {:keys [admission-age episodes]}]
                        (update-in acc [admission-age (-> episodes first :placement)] (fnil inc 0)))
                      periods)]
    (fn [age seed]
      (let [params (get coefs age)]
        (if params
          (let [[ks alphas] (apply map vector params)
                [s1 s2] (rand/split seed)
                category-probs (zipmap ks (d/draw (d/dirichlet {:alphas alphas}) {:seed s1}))]
            (d/draw (d/categorical category-probs) {:seed s2}))
          spec/unknown-placement ;; Fallback - never seen a joiner of this age
          )))))

(defn rejection-model
  "Manage reject sampling for both simulated and projected periods. We have different reject propensities for each.
  Returns true if the sample should be kept."
  [rejection-proportions]
  ;; duration group is measured in years in half-year increments
  (let [{:keys [m-p m-s]} (reduce (fn [{:keys [m-s m-p] :as coll} [[join-age duration-group] {:keys [p s h] :or {p 0.1 s 0.1 h 0.1}}]]
                                    (-> coll
                                        (update :m-s max (/ h s))
                                        (update :m-p max (/ h p))))
                                  {:m-s 1 :m-p 1}
                                  rejection-proportions)
        ]
    (fn [admission-age duration-days provenance]
      (assert (#{"S" "P"} provenance))
      (let [u (rand)
            duration-group (* (Math/floor (/ duration-days (* 365 2.0))) 2.0) U (rand)
            {:keys [h p s] :or {h 0.1 s 0.1 p 0.1}} (get rejection-proportions [admission-age duration-group])
            candidate (if (= provenance "S") s p)
            c (if (= provenance "S") m-s m-p)
            keep? (if (nil? h)
                    false
                    (<= u (/ h (* candidate c))))]
        ;; (println (format "(<= %s (/ %s (* %s %s))) => %s " (str u) (str h) (str c) (str candidate) (str keep?)))
        keep?))))

(defn projection-model
  [candidates]
  (let [periods (->> (reduce (fn [coll {:keys [id reject-ratio] :as candidate}]
                               (-> coll
                                   (update-in [id :candidates] conj candidate)
                                   (update-in [id :c] (fnil max 1) reject-ratio)))
                             {}
                             candidates)
                     (reduce (fn [coll [k {:keys [c candidates]}]]
                               (-> coll
                                   (assoc-in [k :c] c)
                                   (assoc-in [k :candidates] (vec candidates))))
                             {}))]
    (fn [period-id seed]
      (if-let [{:keys [c candidates]} (get periods period-id)]
        (loop [counter 0
               seed seed]
          (let [[s1 s2] (rand/split seed)
                {:keys [reject-ratio] :as candidate} (rand/rand-nth candidates s1)
                u (rand/rand-double s2)]
            (if (or (<= u (* c reject-ratio)) (> counter 100000))
              candidate
              (recur (inc counter) (rand/next-seed s1)))))
        (println "Couldn't complete" period-id)))))

(defn simulation-model
  [candidates]
  (let [periods (->> (reduce (fn [coll {:keys [admission-age reject-ratio] :as candidate}]
                               (-> coll
                                   (update-in [admission-age :candidates] conj candidate)
                                   (update-in [admission-age :c] (fnil max 1) reject-ratio)))
                             {}
                             candidates)
                     (reduce (fn [coll [k {:keys [c candidates]}]]
                               (-> coll
                                   (assoc-in [k :c] c)
                                   (assoc-in [k :candidates] (vec candidates))))
                             {}))]
    (fn [admission-age seed]
      (let [{:keys [c candidates]} (get periods admission-age)]
        (loop [counter 0
               seed seed]
          (let [[s1 s2] (rand/split seed)
                {:keys [reject-ratio] :as candidate} (rand/rand-nth candidates s1)
                u (rand/rand-double s2)]
            (when-not (and c reject-ratio)
              (println (format "*** No C or reject-ratio %s %s %s %s" admission-age c reject-ratio (count candidates))))
            (if (or (<= u (* c reject-ratio)) (> counter 100000))
              (assoc candidate :iterations counter)
              (recur (inc counter) (rand/next-seed s1)))))))))

(defn age-out-model
  [age-out-proportions]
  (fn
    ([admission-age seed]
     (let [p (get-in age-out-proportions [admission-age :marginal])]
       ;; P is probability of aging out
       ;; We return the age out probability
       ;; (println (format "Age out proportion for age %s is %s" admission-age p))
       (if (>= admission-age 17)
         1.0
         (<= (rand/rand-double seed) p))))
    ([admission-age current-age-days seed]
     (let [p (or (get-in age-out-proportions [admission-age :joint-indexed current-age-days])
                 (some (fn [[test-age-days p]]
                         (when (<= test-age-days current-age-days)
                           p))
                       (get-in age-out-proportions [admission-age :joint-pairs]))
                 (get-in age-out-proportions [admission-age :marginal]))]
       (if (>= admission-age 17)
         1.0
         (<= (rand/rand-double seed) p))))))

(defn age-out-projection-model
  [candidates]
  (let [periods (group-by :id candidates)]
    (fn [period-id seed]
      (when-let [candidates (get periods period-id)]
        (rand/rand-nth candidates seed)))))

(defn age-out-simulation-model
  [candidates]
  (let [periods (group-by :admission-age candidates)]
    (fn [admission-age seed]
      (rand/rand-nth (get periods admission-age) seed))))
