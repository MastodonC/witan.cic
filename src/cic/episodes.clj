(ns cic.episodes)

(defn remove-unmodelled-episodes [data]
  (remove (some-fn :uasc (comp #{:V4 :V3} :legal-status) (comp #{:F6} :placement)) data))

(defn remove-f6 [data]
  (remove (comp #{:F6} :placement) data))

(defn remove-stale-rows
  "The raw data may contain multiple open episodes for a child, one per report year that the episode was open.
  We want to remove open episodes from prior report years, since these are stale data.
  This function removes episodes without a cease date from report years prior to the latest."
  [data]
  (let [latest-report-year (->> data (map :report-year) (apply max))]
    (remove #(and (< (:report-year %) latest-report-year) (nil? (:ceased %))) data)))

(defn scrub-episodes
  "Takes a parsed CSV and returns cleansed episodes data"
  [csv]
  (->> csv
       (remove-stale-rows)
       (remove-unmodelled-episodes)
       (remove-f6)))

(defn add-offset
  [offset episodes]
  (map (fn [episode]
         (update episode :offset + offset))
       episodes))

(defn simplify
  [episodes]
  (first (reduce (fn [[seq previous-placement] {:keys [placement] :as episode}]
                   (vector
                    (cond-> seq
                      (not= previous-placement placement)
                      (conj episode))
                    placement))
                 [[] nil]
                 episodes)))
