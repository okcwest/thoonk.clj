(ns thoonk.core-test
  (:use clojure.test
    thoonk.core
    thoonk.util
    thoonk.redis-base)
  (:require [taoensso.carmine :as redis])
  (:import (thoonk.exceptions FeedExists
                              FeedDoesNotExist)))

(deftest test-env-test
  (testing "Checks if Redis is configured correctly for subsequent tests."
    (is (not (nil? (with-redis (redis/info)))))))
    
(deftest test-empty-get-with-redis
    (testing "Does the with-redis macro produce something appropriate?"
        (is (nil? (with-redis (redis/get "thiskeydoesnotexist"))))))

(deftest test-with-redis-commands
    (testing "Basic redis commands"
        (testing "Set a key's value"
            (is (= "OK" (with-redis (redis/set "test" "foo")))))
        (testing "Get the value"
            (is (= "foo" (with-redis (redis/get "test")))))
        (testing "Remove the key"
            (is (= 1 (with-redis (redis/del "test")))))
        (testing "The key is gone"
            (is (nil? (with-redis (redis/get "test")))))))

(deftest test-with-redis-transaction
    (testing "Transactionally complete the block above"
        (let [  result (with-redis-transaction 
                (redis/set "test" "foo")
                (redis/get "test")
                (redis/del "test")
                (redis/get "test"))]
            (testing "Macro returned OK"
                (is (= "OK" (first result))))
            (testing "Results vector is expected"
                (is (= ["OK" "foo" 1 nil] (last result))))
            (testing "Appropriate number of transactions queued"
                (is (= 6 (count result)))
                (is (= "QUEUED" (nth result 1)))
                (is (= "QUEUED" (nth result 2)))
                (is (= "QUEUED" (nth result 3)))
                (is (= "QUEUED" (nth result 4)))))))

(deftest test-feed-create-delete
    (testing "Create a feed and check its existence"
        (try (delete-feed "nosuchfeed")
          (catch FeedDoesNotExist f nil)) ; this feed should start out non-existent
        (is (not (feed-exists "nosuchfeed")))
        (is (create-feed "nosuchfeed" {:type :feed}))
        (is (feed-exists "nosuchfeed"))
        (is (not (nil? (get-feed "nosuchfeed"))))
        (is (delete-feed "nosuchfeed"))
        (is (not (feed-exists "nosuchfeed")))))

