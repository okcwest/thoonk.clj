(ns thoonk.core-test
  (:use clojure.test
        thoonk.core)
  (:require [taoensso.carmine :as redis]))

(deftest test-env-test
  (testing "Checks if Redis is configured correctly for subsequent tests."
    (is (not (nil? (with-redis (redis/info)))))))
                      