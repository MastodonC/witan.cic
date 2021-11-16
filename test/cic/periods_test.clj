(ns cic.periods-test
  (:require [cic.periods :as sut]
            [cic.random :as random]
            [cic.time :as time]
            [clojure.test :refer :all]))

(defn age-out-model
  [proportions]
  (fn
    ([age seed]
     true)
    ([age age-days seed]
     true)))

(defn projection-model
  [candidates]
  (fn [period-id seed]
    {:duration 20
     :episodes-edn "[{:placement :Q2 :offset 0}]"}))

(defn age-out-projection-model
  [candidates]
  (fn [period-id seed]
    {:duration 18000
     :episodes-edn "[{:placement :Q2 :offset 0}]"}))

(def test-periods
  [{:period-id "abc"
    :beginning (time/make-date 2010 2 1)
    :open? true
    :admission-age 0
    :birthday (time/make-date 2010 1 1)
    :duration 10}])

(deftest close-open-periods-test
  (let [periods test-periods
        age-out-model (age-out-model {})
        projection-model (projection-model [])
        age-out-projection-model (age-out-projection-model [])
        seed (random/seed 42)]
    (testing "age out end is day before 18th birthday"
        (is (= [{:beginning (time/make-date 2010 2 1)
                 :admission-age 0
                 :birthday (time/make-date 2010 1 1)
                 :duration 6542
                 :episodes [{:placement :Q2 :offset 0}]
                 :period-id "abc"
                 :open? false
                 :provenance "P"
                 :end (time/make-date 2027 12 31)}]
               (sut/close-open-periods periods projection-model age-out-model age-out-projection-model seed))))))

(deftest segment-test
  (let [period {:beginning (time/make-date 2010 2 1)
                :end (time/make-date 2027 12 31)
                :birthday (time/make-date 2010 1 1)
                :duration 6542
                :episodes [{:placement :Q1 :offset 0}
                           {:placement :Q2 :offset 2000}
                           {:placement :A6 :offset 4000}
                           {:placement :P1 :offset 6000}]}]
    (testing "long period gets broken into 6 year segments"
      (is (= [{:age-days 31
	       :date (time/make-date 2010 2 1)
	       :from-placement :Q1
	       :age 0
	       :to-placement :Q2
	       :aged-out? false
	       :duration 2190
	       :care-days 0
	       :terminal? false
	       :episodes
	       [{:placement :Q1, :offset 0} {:placement :Q2, :offset 2000}]
	       :join-age-days 31
	       :initial? true}
	      {:age-days 2221
	       :date (time/make-date 2016 1 31)
	       :from-placement :Q2
	       :age 6
	       :to-placement :A6
	       :aged-out? false
	       :duration 2190
	       :care-days 2190
	       :terminal? false
	       :episodes
	       [{:placement :Q2, :offset 0} {:placement :A6, :offset 1810}]
	       :join-age-days 31
	       :initial? false}
	      {:age-days 4411,
	       :date (time/make-date 2022 1 29)
	       :from-placement :A6
	       :age 12
	       :to-placement :P1
	       :aged-out? true
	       :duration 2162
	       :care-days 4380
	       :terminal? true
	       :episodes
	       [{:placement :A6, :offset 0} {:placement :P1, :offset 1620}]
	       :join-age-days 31
	       :initial? false}]
             (sut/segment period))))))
