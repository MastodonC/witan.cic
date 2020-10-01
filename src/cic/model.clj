(ns cic.model
  (:require [cic.io.read :as read]
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
        periods-in (->> (periods/to-mapseq periods)
                        (map #(-> %
                                  (update :placement name)
                                  (update :beginning time/date-as-string)
                                  (update :end (fn [end] (when end (time/date-as-string end))))
                                  (update :report-date time/date-as-string)
                                  (update :birthday time/date-as-string)
                                  (set/rename-keys {:report-date :report_date :period-id :period_id})))
                        (write/mapseq->csv!)
                        (str))
        script "src/close-open-cases.R"
        seed-long (rand/rand-long seed)]
    (rscript/exec script periods-in clusters-out (time/date-as-string project-from) (str (Math/abs seed-long)))
    (read/knn-closed-cases clusters-out)))
