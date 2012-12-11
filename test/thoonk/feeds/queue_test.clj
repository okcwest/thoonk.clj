(ns thoonk.feeds.queue-test
  (:use [clojure.test]
        [thoonk.core]
        [thoonk.feeds.feed]
        [thoonk.feeds.queue]
        [clojure.tools.logging])
  (:require [thoonk.util :as util])
  (:import (thoonk.exceptions  Empty)))

(deftest test-queue-schemas []
  (testing "Make sure we can create a queue with all expected schemata"
    (let [created (create-feed "testqueue" {:type :queue})
          queue (get-feed "testqueue")
          schemas (get-schemas queue)]
     (is queue) ; verify we got something
     (is (contains? schemas "feed.ids:testqueue")) ; should all be there
     (is (contains? schemas "feed.items:testqueue"))
     (is (contains? schemas "feed.publish:testqueue"))
     (is (contains? schemas "feed.publishes:testqueue"))
     (is (contains? schemas "feed.retract:testqueue"))
     (is (contains? schemas "feed.config:testqueue"))
     (is (contains? schemas "feed.edit:testqueue"))
     (is (not (contains? schemas "feed.stalled:testqueue"))))) ; job only
  ; clean up
  (delete-feed "testqueue"))

(deftest test-push-pull-retract-queue []
  (testing "Read and write with a queue."
    (let [created (create-feed "testqueue" {:type :queue})
          queue (get-feed "testqueue")
          first-id (push queue "first-in")
          second-id (push queue "second-in")
          third-id (push queue "third-in-priority" true)
          fourth-id (push queue "fourth-in")]
      ; get the ids and verify the items are as we expect
      (let [ids (get-ids queue)]
        (is (= 4 (count ids)))
        ; check the order
        (is (= fourth-id (nth ids 0)))
        (is (= second-id (nth ids 1)))
        (is (= first-id (nth ids 2)))
        (is (= third-id (nth ids 3))))
      ; and the values, by random access
      (is (= "first-in" (get-item queue first-id)))
      (is (= "second-in" (get-item queue second-id)))
      (is (= "third-in-priority" (get-item queue third-id)))
      (is (= "fourth-in" (get-item queue fourth-id)))
      ; retract an item
      (retract queue second-id)
      ; check the ids again
      (let [ids (get-ids queue)]
        (is (= 3 (count ids)))
        ; check the order
        (is (= fourth-id (nth ids 0)))
        (is (= first-id (nth ids 1)))
        (is (= third-id (nth ids 2))))
      ; pull the items
      (is (= "third-in-priority" (pull queue)))
      (is (= "first-in" (pull queue)))
      ; second is gone
      (is (= "fourth-in" (pull queue)))
      ; make sure everything is gone
      (is (= 0 (count (get-ids queue))))
      (is (= 0 (count (get-all queue))))
      ; blocking call should time out appropriately.
      (let [t0 (util/get-time)]
        (is (thrown? Empty (pull queue 1))) ; block while requesting
        (let [t1 (util/get-time)
              elapsed (- t1 t0)]
          ; allow a bit of tolerance on the top side. should never time out early.
          (is (> 3000 elapsed 1000))))))
  ; clean up
  (delete-feed "testqueue"))
