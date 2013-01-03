(defproject thoonk "0.1.0"
  :description "Thoonk client implemented in Clojure"
  :url "https://github.com/okcwest/thoonk.clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [com.taoensso/carmine "0.11.3"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jdmk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [org.clojure/tools.logging "0.2.3"]]
  :aot [#"thoonk.exceptions.*"]
  :source-paths ["src"]
  :test-paths ["test"]
  :profiles {:dev {:plugins [[jonase/kibit "0.0.4"]]}}
  )
