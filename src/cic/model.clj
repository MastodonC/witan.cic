(ns cic.model
  (:require [cic.io.read :as read]
            [cic.io.write :as write]
            [cic.random :as rand]
            [cic.rscript :as rscript]
            [cic.spec :as spec]
            [cic.time :as time]
            [clj-time.core :as t]
            [clojure.math.combinatorics :as c]
            [kixi.stats.core :as k]
            [kixi.stats.distribution :as d]
            [kixi.stats.math :as m]
            [kixi.stats.protocols :as p]))

(defn joiners-model
  "Given the date of a joiner at a particular age,
  returns the interval in days until the next joiner"
  [{:keys [model-coefs]}]
  (fn [age join-after previous-joiner seed]
    (loop [seed seed iter 1]
      (let [day (t/in-days (t/interval (t/epoch) previous-joiner))
            intercept (get model-coefs "(Intercept)")
            a (get model-coefs (str "admission_age" age) 0.0)
            b (get model-coefs "quarter")
            c (get model-coefs (str "quarter:admission_age" age) 0.0)
            n-per-quarter (m/exp (+ intercept a (* b day) (* c day)))
            n-per-day (max (/ n-per-quarter 91.3125) (/ 1 365.25)) ;; Rate per day
            sample (p/sample-1 (d/exponential {:rate n-per-day}) seed)
            join-date (time/days-after previous-joiner sample)]
        (if (>= iter 50)
          (do (println (format "Exceeded iterations for joiner age %s" age))
              sample)
          (if (time/>= join-date join-after)
            sample
            (recur (second (rand/split seed)) (inc iter))))))))

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

(defn joiner-placements-model
  [coefs]
  (fn [age]
    (let [params (get coefs age)]
      (if params
        (let [[ks alphas] (apply map vector params)
              category-probs (zipmap ks (d/draw (d/dirichlet {:alphas alphas})))]
          (d/draw (d/categorical category-probs)))
        spec/unknown-placement ;; Fallback - never seen a joiner of this age
        ))))

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
  [cease-model periods episodes-from episodes-to]
  (let [joiner-placements (joiner-placements-model
                           (reduce (fn [acc {admission-age :admission-age [{first-placement :placement}] :episodes}]
                                     (update-in acc [admission-age first-placement] (fnil inc 0)))
                                   {}
                                   (filter #(time/between? (:beginning %) episodes-from episodes-to) periods)))
        joiner-clusters (reduce (fn [acc {admission-age :admission-age [{first-placement :placement}] :episodes cluster :cluster}]
                                  (update-in acc [admission-age cluster] (fnil inc 0)))
                                {}
                                (filter #(time/between? (:beginning %) episodes-from episodes-to) periods))

        transitions (reduce (fn [acc {:keys [birthday beginning episodes open? end]}]
                              (reduce (fn [acc [{offset-a :offset from :placement} {offset-b :offset to :placement}]]
                                        (let [age (time/year-interval birthday (time/days-after beginning offset-a))]
                                          (if offset-b
                                            (-> (update-in acc [:age-from-to [age from] to :n] (fnil inc 0))
                                                (update-in [:age-from-to [age from] to :durations] (fnil conj []) (- offset-b offset-a))
                                                (update-in [:age-to age to :n] (fnil inc 0))
                                                (update-in [:age-to age to :durations] (fnil conj []) (- offset-b offset-a)))
                                            #_(if (not open?)
                                                (-> (update-in acc [:age-from-to age from :OUT :n] (fnil inc 0))
                                                    (update-in [:age-from-to age from :OUT :durations] conj (- (time/days-after beginning end) offset-a))
                                                    (update-in [:age-to age :OUT :n] (fnil inc 0))
                                                    (update-in [:age-to age :OUT :durations] conj (- (time/days-after beginning end) offset-a))
                                                    (update-in [:from-to from :OUT :n] (fnil inc 0))
                                                    (update-in [:from-to from :OUT :durations] conj (- (time/days-after beginning end) offset-a)))
                                                acc)
                                            acc)))
                                      acc
                                      (partition 2 1 episodes)))
                            {}
                            periods)]
    (fn [{:keys [episodes duration beginning birthday cluster]} seed]
      (let [age (time/year-interval birthday beginning)
            cluster (if cluster cluster (dirichlet-categorical (get joiner-clusters age)))
            episodes (if (seq episodes)
                       episodes
                       (let [age (time/year-interval birthday beginning)
                             placement (joiner-placements age)]
                         [{:offset 0 :placement placement}]))
            {current-placement :placement start-offset :offset} (last episodes)
            current-open-duration (if duration (- duration start-offset) 0)
            max-total-duration (dec (time/day-interval beginning (time/years-after birthday 18)))]
        (loop [current-offset start-offset
               placement current-placement
               episodes (vec episodes)]
          (let [age (time/year-interval birthday (time/days-after beginning current-offset))
                [next-placement placement-duration] (loop [iter 0 age age]
                                                      (let [transitions (if (pos? current-open-duration)
                                                                          (filter-transitions transitions current-open-duration)
                                                                          transitions)
                                                            transitions-summary (or (get-in transitions [:age-from-to [age current-placement]])
                                                                                    (get-in transitions [:age-to age])
                                                                                    (get-in transitions [:age-to (dec age)]))]
                                                        
                                                        (if (empty? transitions-summary)
                                                          (if (zero? age)
                                                            [:OUT current-open-duration]
                                                            (recur (inc iter) (dec age)))
                                                          (let [[next-placements options] (apply map vector transitions-summary)
                                                                placement-counts (map :n options)
                                                                next-placement (try (dirichlet-categorical (zipmap next-placements placement-counts))
                                                                                    (catch Exception e
                                                                                      (println "Exception" age current-placement current-open-duration next-placements placement-counts)
                                                                                      (first next-placements)))
                                                                placement-duration (some-> (get-in transitions-summary [next-placement :durations]) shuffle first)]
                                                            (if placement-duration
                                                              [next-placement placement-duration]
                                                              (recur (inc iter) age))))))
                next-offset (+ current-offset placement-duration)]
            (if (or (= next-placement :OUT)
                    (>= next-offset max-total-duration)
                    (cease-model birthday beginning next-offset seed))
              {:episodes episodes :duration (min next-offset max-total-duration)}
              (recur next-offset next-placement (conj episodes {:offset next-offset :placement next-placement})))))))))

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
        (-> (time/days-before join-date (int (p/sample-1 (d/uniform {:a 0 :b 366}) seed)))
            (time/years-before age))))))
