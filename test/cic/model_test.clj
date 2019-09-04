(ns cic.model-test
  (:require [cic.model :refer :all]
            [clojure.test :refer :all]
            [cic.core :as c]
            [cic.projection :as p]
            [clojure.test.check.random :as r]))

(def example-data '({:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-02-18", :id "120", :report-year "2017", :placement "K1", :report-date "2017-02-10"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-04-18", :id "120", :report-year "2017", :placement "K2", :report-date "2017-02-18"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2015-02-18", :id "120", :report-year "2015", :placement "U1", :report-date "2015-02-10"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2014-02-18", :id "120", :report-year "2014", :placement "U1", :report-date "2014-02-10"}
                    {:sex "2", :care-status "B1", :legal-status "C1", :uasc "False", :dob "2000", :ceased nil, :id "121", :report-year "2017", :placement "U1", :report-date "2017-02-10"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased "2017-05-18", :id "122", :report-year "2017", :placement "U2", :report-date "2017-05-10"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased nil, :id "122", :report-year "2018", :placement "U2", :report-date "2018-05-10"}
                    {:sex "2", :care-status "N1", :legal-status "C2", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                    {:sex "2", :care-status "N1", :legal-status "V3", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                    {:sex "2", :care-status "N1", :legal-status "V4", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}))

(def example (p/prepare-ages (->> example-data
                                  (map c/format-episode)
                                  c/episodes
                                  c/episodes->periods)
                             (r/make-random 50)))

(deftest update-fuzzy-test
  (let [age (:admission-age (first example))
        fuzzy-age [(- age 1) age (+ age 1)]
        yrs (/ (:duration (first example)) 365.0)
        fuzzy-yrs (mapv int [(Math/floor yrs) (Math/ceil yrs)])
        episodes (:episodes (first example))
        result (update-fuzzy {} [age yrs] conj episodes)]
    (testing "returns range of ages close to example"
      (is (= (vec (distinct (map #(-> % first first) result)))
             fuzzy-age)))
    (testing "returns range of year counts close to example"
      (is (= (vec (distinct (map #(-> % first second) result)))
             fuzzy-yrs)))))
