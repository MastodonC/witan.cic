(ns cic.spec)

(def unknown-placement
  :NA)

(def placements
  [:Q2 :K2 :Q1 :R2 :P2 :H5 :R5 :R1 :A6 :P1 :Z1 :S1 :K1
   :A4 :T4 :M3 :A5 :A3 :R3 :M2 :T0 unknown-placement])

(def ages
  (range 0 18))

(def placement-ages
  (for [placement placements
        age ages]
    (str (name placement) "-" age)))
