(ns cic.core-test
  (:require [clojure.test :refer :all]
            [cic.core :refer :all]))

(def data (map format-episode '({:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-02-18", :id "120", :report-year "2017", :placement "K1", :report-date "2017-02-10"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-04-18", :id "120", :report-year "2017", :placement "K2", :report-date "2017-02-18"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2015-02-18", :id "120", :report-year "2015", :placement "U1", :report-date "2015-02-10"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2014-02-18", :id "120", :report-year "2014", :placement "U1", :report-date "2014-02-10"}
                                {:sex "2", :care-status "B1", :legal-status "C1", :uasc "False", :dob "2000", :ceased nil, :id "121", :report-year "2017", :placement "U1", :report-date "2017-02-10"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased "2017-05-18", :id "122", :report-year "2017", :placement "U2", :report-date "2017-05-10"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased nil, :id "122", :report-year "2018", :placement "U2", :report-date "2018-05-10"}
                                {:sex "2", :care-status "N1", :legal-status "C2", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                                {:sex "2", :care-status "N1", :legal-status "V3", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                                {:sex "2", :care-status "N1", :legal-status "V4", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"})))

(def projection-start
  (->> (mapcat (juxt :report-date :ceased) (episodes data))
       (remove nil?)
       (sort)
       (last)))

(deftest remove-unmodelled-episodes-test
  (testing "3 unmodelled episodes removed corresponding to UASC, V3 and V4"
    (is (= 7 (count (remove-unmodelled-episodes data))))))

#_(deftest episodes-test
    (testing "remove invalid records"
      (is (= 5 (count (episodes data))))))


#_(deftest assoc-period-id-test
    (testing "id 120 consists of 3 periods"
      (let [child (filter #(= (:child-id %) 120) (episodes data))
            result (assoc-period-id child)]
        (is (= 3 (count (distinct result))))
        (is (= '("120-0" "120-1" "120-2") (map :period-id result))))))


#_(deftest summarise-periods-at-test
    (let [result (->> (episodes data)
                      (assoc-period-id)
                      (group-by :period-id)
                      (vals)
                      (map #(summarise-periods-at % projection-start)))]
      (testing "a single open episode"
        (is (= 1 (count (filter #(= true (:open? %)) result)))))
      (testing "duration in care calculated"
        (is (= 1550 (:duration (first (filter #(= (:period-id %) "120-0") result))))))
      (testing "multiple episodes in a period"
        (is (= [{:offset 0, :placement :K1} {:offset 8, :placement :K2}]
               (->> result
                    (filter #(= (:period-id %) "120-2"))
                    first
                    :episodes))))))
