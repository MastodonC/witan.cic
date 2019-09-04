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

(def duration-data {0 [[0 0 0][1 6 17][10 17 25][17 21 51][19 39 68][35 56 83][50 77 107][67 84 117][79 107 126][84 115 133][107 121 137][115 130 139][121 135 147][129 138 152][133 141 156][137 148 161][139 152 165][145 157 171][151 161 174][154 165 181][158 171 187][162 174 190][167 181 201][171 186 207][174 190 210][181 199 218][186 203 225][189 210 232][195 214 237][202 224 242][209 230 244][213 235 251][223 239 256][228 243 263][233 245 273][238 252 280][242 258 288][245 265 303][251 278 313][253 282 318][263 290 330][270 305 337][279 314 349][286 323 357][300 332 365][309 339 370][316 349 380][327 357 390][336 365 396][344 372 407][356 380 412][362 391 418][368 396 425][377 407 429][383 412 439][392 418 446][399 426 452][410 431 463][417 440 472][425 448 476][428 454 489][435 464 498][443 474 504][452 477 514][461 490 519][468 498 524][476 505 534][486 515 543][497 521 548][504 526 556][514 540 563][519 547 581][526 551 590][540 559 599][546 566 616][550 590 623][559 598 637][566 613 646][590 619 663][598 636 664][614 643 677][623 663 679][637 664 685][646 677 699][663 680 721][670 687 744][678 700 770][683 734 794][696 760 811][721 779 827][748 802 843][776 817 887][802 840 938][821 869 974][843 938 1037][897 974 1094][948 1072 1223][1033 1104 1343][1104 1320 1932][1343 1707 4433][6935 6935 6935]]})

(deftest duration-model-test
  (let [duration-model-0 (partial (duration-model duration-data) 0)
        list-durations (reduce concat (reduce into [] (vals duration-data)))
        minimum (apply min list-durations)
        maximum (apply max list-durations)]
    (testing "duration for age 0 always falls between min and max when randomly sampled 1000 times"
      (is (every? #(and (>= % minimum) (<= % maximum))
                  (repeat 1000 (duration-model-0 (r/make-random (rand-int 10000)))))))))
