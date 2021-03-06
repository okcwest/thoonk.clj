(ns thoonk.core-test
  (:use clojure.test
    thoonk.core
    thoonk.util
    thoonk.redis-base
    clojure.tools.logging)
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

; following are tests for pubsub

(def handled (atom {})) ; track pubsub events properly caught

; some test handlers to associate that will count caught events
(defn handle-create
  [name]
  (let [old (or (:created @handled) [])]
    (debug "Called create handler")
    ;))
    (swap! handled assoc :created (conj old name))))

(defn handle-config
  [name]
  (let [old (or (:configured @handled) [])]
    (debug "Called config handler")
    ;))
    (swap! handled assoc :configured (conj old name))))

(defn handle-delete
  [name]
  (let [old (or (:deleted @handled) [])]
    (debug "Called delete handler")
    ;))
    (swap! handled assoc :deleted (conj old name))))

(deftest test-pub-sub
  (testing "Try to catch an event from pubsub"
    ; in the initial state we have created nothing yet
    (is (nil? (:created @handled)))
    (is (nil? (:deleted @handled)))
    ; set up a listener and register our handlers
    (let [listener (create-listener)]
      ;(prn listener)
      (is (not (nil? listener)))
      ;(Thread/sleep 1000) ; sleep through the subscribe announcements
      (register-handler listener "create" handle-create)
      (register-handler listener "delete" handle-delete)
      (register-handler listener "config" handle-config)
      ; create a feed
      (create-feed "testfeed" {:type :feed})
      (Thread/sleep 500) ; give the handler a chance to fire
      ; should now have handled a create and a config event with handle-create.
      (is (some #{"testfeed"} (:created @handled)))
      (is (some #{"testfeed"} (:configured @handled)))
      (is (not (some #{"testfeed"} (:deleted @handled))))
      ; delete it and make sure that's handled too.
      (delete-feed "testfeed")
      (Thread/sleep 500)
      (is (some #{"testfeed"} (:deleted @handled)))
      ; clean up
      (terminate-listener listener))))


