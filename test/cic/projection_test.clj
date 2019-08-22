(ns cic.projection-test
  (:require [cic.projection :refer :all]
            [clojure.test :refer :all]
            [cic.core :as c]
            [cic.model :as m]
            [clj-time.core :as t]
            [clojure.test.check.random :as r]))

(def example (c/episodes->periods (c/episodes (map c/format-episode '({:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-02-18", :id "120", :report-year "2017", :placement "K1", :report-date "2017-02-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2017-04-18", :id "120", :report-year "2017", :placement "K2", :report-date "2017-02-18"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2015-02-18", :id "120", :report-year "2015", :placement "U1", :report-date "2015-02-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1999", :ceased "2014-02-18", :id "120", :report-year "2014", :placement "U1", :report-date "2014-02-10"}
                                                                      {:sex "2", :care-status "B1", :legal-status "C1", :uasc "False", :dob "2000", :ceased nil, :id "121", :report-year "2017", :placement "U1", :report-date "2017-02-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased "2017-05-18", :id "122", :report-year "2017", :placement "U2", :report-date "2017-05-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "False", :dob "1998", :ceased nil, :id "122", :report-year "2018", :placement "U2", :report-date "2018-05-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "C2", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "V3", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"}
                                                                      {:sex "2", :care-status "N1", :legal-status "V4", :uasc "True", :dob "1999", :ceased "2017-07-18", :id "124", :report-year "2017", :placement "U2", :report-date "2017-06-10"})))))

(def d-model (m/duration-model {0 [[0 0 0] [1 6 17] [35 56 83]]
                                1 [[0 0 0] [1 6 17] [35 56 83]]
                                2 [[0 0 0] [1 6 17] [35 56 83]]
                                3 [[0 0 0] [1 6 17] [35 56 83]]
                                4 [[0 0 0] [1 6 17] [35 56 83]]
                                5 [[0 0 0] [1 6 17] [35 56 83]]
                                6 [[0 0 0] [1 6 17] [35 56 83]]
                                7 [[0 0 0] [1 6 17] [35 56 83]]
                                8 [[0 0 0] [1 6 17] [35 56 83]]
                                9 [[0 0 0] [1 6 17] [35 56 83]]
                                10 [[0 0 0] [1 6 17] [35 56 83]]
                                11 [[0 0 0] [1 6 17] [35 56 83]]
                                12 [[0 0 0] [1 6 17] [35 56 83]]
                                13 [[0 0 0] [1 6 17] [35 56 83]]
                                14 [[0 0 0] [1 6 17] [35 56 83]]
                                15 [[0 0 0] [1 6 17] [35 56 83]]
                                16 [[0 0 0] [1 6 17] [35 56 83]]
                                17 [[0 0 0] [1 6 17] [35 56 83]]
                                18 [[0 0 0] [1 6 17] [35 56 83]]}))

(deftest prepare-ages-test
  (let [result (prepare-ages example (r/make-random 50))]
    (testing "birthday is within correct year"
      (is (= 14 (t/in-years (t/interval (:birthday (first result)) (t/date-time 2014))))))
    (testing "admission age in correctly calculated"
      (is (= (t/in-years (t/interval (t/date-time 1999) (t/date-time 2014)))
             (:admission-age (first result)))))))
