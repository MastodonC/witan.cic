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
            [kixi.stats.protocols :as p]))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [{:keys [model-coefs]}]
  (fn [age join-after previous-joiner seed]
    (loop [seed seed sample-adjustment 0]
      (let [day (t/in-days (t/interval (t/epoch) previous-joiner))
            intercept (get model-coefs "(Intercept)")
            a (get model-coefs (str "admission_age" age) 0.0)
            b (get model-coefs "quarter")
            c (get model-coefs (str "quarter:admission_age" age) 0.0)
            n-per-quarter (m/exp (+ intercept a (* b day) (* c day)))
            n-per-day (max (/ n-per-quarter 91.3125) (/ 1 365.25)) ;; Rate per day
            sample (+ sample-adjustment (p/sample-1 (d/exponential {:rate n-per-day}) seed))
            join-date (time/days-after previous-joiner sample)]
        (if (time/>= join-date join-after)
          sample
          (recur (second (rand/split seed)) sample-adjustment))))))

(defn joiners-model-gen
  "Wraps R to trend joiner rates into the future."
  [periods project-to seed]
  (let [script "src/joiners.R"
        input (str (rscript/write-periods! periods))
        output (str (write/temp-file "file" ".csv"))
        seed-long (rand/rand-long seed)]
    (println script input output (time/date-as-string project-to) (str (Math/abs seed-long)))
    (rscript/exec script input output
                  (time/date-as-string project-to)
                  (str (Math/abs seed-long)))
    (-> (read/joiner-csv output)
        (joiners-model))))

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

(defn phase-duration-quantiles-model
  [coefs]
  (fn [first-phase?]
    (let [quantiles (if first-phase?
                      (:first coefs)
                      (:rest coefs))]
      (rand-nth quantiles))))

(defn phase-transitions-model
  [coefs]
  (fn [first-transition? age placement]
    (let [params  (get coefs {:first-transition first-transition?
                              :transition-age age
                              :transition-from placement})]
      (if params
        (let [[ks alphas] (apply map vector params)
              category-probs (zipmap ks (d/draw (d/dirichlet {:alphas alphas})))]
          #_(println "Found phase transition params for age" age "placement" placement "first transition" first-transition?)
          (d/draw (d/categorical category-probs)))
        (do #_(println "Didn't find phase transition params for age" age "placement" placement "first transition" first-transition?)
            placement)))))

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
  (let [age-groups (group-by :admission-age periods)]
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
  [x n scale]
  (let [p (/ x n)
        dist (d/binomial {:n (/ n scale) :p p})]
    (long (* scale (d/draw dist)))))

(defn get-matched-segment
  [feature-fn feature-vec segments]
  (min-key'
   (fn [segment]
     (euclidean-distance (feature-fn segment) feature-vec))
   segments))

(def max-age-days (* 365 18))

(def jitter-scale 1) ;; Higher is more jittering

(defn periods-simulations
  [periods offset-segments]
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
                                           (let [join-age-days (jitter-binomial join-age-days max-age-days jitter-scale)
                                                 care-days (jitter-binomial duration max-duration jitter-scale)
                                                 segment (get-matched-segment (juxt :join-age-days :care-days) [join-age-days care-days]
                                                                              (get offset-segments [offset last-placement]))]
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
  [{:keys [episodes birthday beginning duration period-id provenance] :as period} offset-segments rejection-model]
  (assert provenance)
  (let [admission-age (time/year-interval birthday beginning)
        max-duration (dec (time/day-interval beginning (time/years-after birthday 18)))
        init-duration duration
        init-placement (-> episodes last :placement)
        init-episodes episodes
        init-offset (rem duration periods/segment-interval)
        init-initial? (< duration periods/segment-interval)
        [episodes total-duration iterations]
        (loop [total-duration init-duration
               last-placement init-placement
               all-episodes init-episodes
               offset init-offset
               initial? init-initial?
               counter 0]
          (let [age-days (time/day-interval birthday (time/days-after beginning total-duration))
                age-days (jitter-binomial age-days max-age-days jitter-scale)
                join-age-days (time/day-interval birthday beginning)
                join-age-days (jitter-binomial join-age-days max-age-days jitter-scale)
                ;; care-days (jitter-binomial total-duration max-duration jitter-scale)
                care-days total-duration
                {:keys [terminal? episodes duration to-placement aged-out?] :as sample}
                (get-matched-segment (juxt :join-age-days :care-days) [join-age-days care-days]
                                     (or (get offset-segments [offset last-placement])
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
                 total-duration'
                 counter]

                :else
                (recur total-duration'
                       to-placement
                       episodes
                       0               ;; Zero offset
                       (boolean false) ;; Always return false
                       (inc counter))))))]
    (when (and episodes total-duration)
      (-> period
          (assoc :episodes (episodes/simplify episodes))
          (assoc :duration total-duration)
          (assoc :open? false)
          (assoc :end (time/days-after beginning total-duration))
          (assoc :iterations iterations)))))

(defn markov-placements-model
  [periods rejection-model learn-from learn-to close-open-periods?]
  (let [offset-segments (offset-groups periods learn-from learn-to close-open-periods?)]
    (fn [{:keys [episodes birthday beginning duration period-id] :as period}]
      (markov-period period offset-segments rejection-model))))

(defn joiner-placements-model
  [periods]
  (let [coefs (reduce (fn [acc {:keys [admission-age episodes]}]
                        (update-in acc [admission-age (-> episodes first :placement)] (fnil inc 0)))
                      periods)]
    (fn [age]
      (let [params (get coefs age)]
        (if params
          (let [[ks alphas] (apply map vector params)
                category-probs (zipmap ks (d/draw (d/dirichlet {:alphas alphas})))]
            (d/draw (d/categorical category-probs)))
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
    (fn [period-id]
      (if-let [{:keys [c candidates]} (get periods period-id)]
        (loop [counter 0]
          (let [{:keys [reject-ratio] :as candidate} (rand-nth candidates)
                u (rand)]
            (if (or (<= u (* c reject-ratio)) (> counter 100000))
              candidate
              (recur (inc counter)))))
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
    (fn [admission-age]
      (let [{:keys [c candidates]} (get periods admission-age)]
        (loop [counter 0]
          (let [{:keys [reject-ratio] :as candidate} (rand-nth candidates)
                u (rand)]
            (if (or (<= u (* c reject-ratio)) (> counter 100000))
              (assoc candidate :iterations counter)
              (recur (inc counter)))))))))
