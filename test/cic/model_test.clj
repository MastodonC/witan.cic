(ns cic.model-test
  (:require [cic.model :as sut]
            [cic.random :as random]
            [cic.time :as time]
            [clojure.test :refer :all]))

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

