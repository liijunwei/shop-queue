(defproject shop-queue "0.1.0-SNAPSHOT"
  :description "Meituan-style store queue ticketing (prototype)"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.2"]
                 [http-kit "2.8.0"]
                 [cheshire "5.13.0"]
                 [com.github.seancorfield/next.jdbc "1.3.883"]
                 [org.xerial/sqlite-jdbc "3.43.0.0"]]
  :main ^:skip-aot shop-queue.core
  :target-path "target/%s"
  :aliases {"deploy" ["do" "test," "uberjar," "run" "-m" "shop-queue.deploy"]}
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
