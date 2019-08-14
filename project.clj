(defproject cic "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/data.csv "0.1.4"]
                 [camel-snake-kebab "0.4.0"]
                 [kixi/stats "0.5.2"]
                 [clj-time "0.15.0"]]
  :main ^:skip-aot cic.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
