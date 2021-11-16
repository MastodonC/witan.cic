(ns cic.model-test
  (:require [cic.model :as sut]
            [cic.random :as random]
            [cic.time :as time]
            [clojure.test :refer :all]))

(defn rejection-model
  [rejection-proportions]
  (fn [admission-age duration-days provenance]
    true))

(deftest joiners-model-test
  (let [coefs {"(Intercept)" 0
               "admission_age0" 0.5
               "quarter" 0
               "quarter:admission_age0" 0}
        project-from (time/make-date 2021 1 1)
        project-to (time/make-date 2026 1 1)
        seed (random/seed 42)
        model (sut/joiners-model {:model-coefs coefs} project-from project-to 1 seed)]
    (is (= 15.233248281888217
           (model 0 project-from project-from seed)))))

(deftest markov-period-test
  (let [period {:episodes [{:placement :Q1 :offset 0}]
                :birthday (time/make-date 2010 1 1)
                :beginning (time/make-date 2020 2 1)
                :duration 2000
                :period-id "abc"
                :provenance "S"
                :seed (random/seed 42)}
        offset-segments {[2000 :Q1] [{:terminal? true
                                      :episodes [{:placement :Q1 :offset 0}]
                                      :duration 6500
                                      :to-placement :Q1
                                      :aged-out? true}]}
        rejection-model (rejection-model {})]
    (is (= {:beginning (time/make-date 2020 2 1)	  
	    :birthday (time/make-date 2010 1 1)
	    :duration 2890
	    :episodes [{:placement :Q1, :offset 0}]
	    :period-id "abc"
	    :open? false
	    :iterations 0
	    :provenance "S"
	    :end (time/make-date 2027 12 31)}
           (dissoc (sut/markov-period period offset-segments rejection-model) :seed)))))

(deftest projection-model-test
  (let [candidates [{:id "abc" :reject-ratio 1}
                    {:id "abc" :reject-ratio 0.5}]
        model (sut/projection-model candidates)
        seed (random/seed 42)]
    (is (= {:id "abc" :reject-ratio 1 :iterations 3}
           (model "abc" seed)))))

(deftest simulation-model-test
  (let [candidates [{:admission-age 0 :reject-ratio 1}
                    {:admission-age 0 :reject-ratio 0.5}]
        model (sut/simulation-model candidates)
        seed (random/seed 21)]
    (is (= {:admission-age 0 :reject-ratio 1 :iterations 0}
           (model 0 seed)))))
