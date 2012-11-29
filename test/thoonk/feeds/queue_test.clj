(ns thoonk.feeds.queue-test
  (:use [clojure.test]
        [thoonk.core]
        [thoonk.feeds.feed]
        [thoonk.feeds.queue])
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

(deftest test-push-pull-queue []
  (testing "Read and write with a queue."
    (let [created (create-feed "testqueue" {:type :queue})
          queue (get-feed "testqueue")]
      (is (push queue "first-in"))
      (is (push queue "second-in"))
      (is (push queue "third-in-priority" true))
      ; get the ids and verify the items are as we expect
      (let [ids (get-ids queue)]
        (is (= 3 (count ids)))
        (is (= "second-in" (get-item queue (nth ids 0))))
        (is (= "first-in" (get-item queue (nth ids 1))))
        (is (= "third-in-priority" (get-item queue (nth ids 2)))))
      ; pull the items
      (is (= "third-in-priority" (pull queue)))
      (is (= "first-in" (pull queue)))
      (is (= "second-in" (pull queue)))
      ; make sure everything is gone
      (is (= 0 (count (get-ids queue))))
      (is (= 0 (count (get-all queue))))
      ; blocking call should time out appropriately.
      (let [t0 (.getTime (java.util.Date.))
            pulled (try (pull queue 5)
                      (catch Empty e nil)) ; we expect this exception
            t1 (.getTime (java.util.Date.))
            elapsed (- t1 t0)]
        (is (> 6000 elapsed 4000)) ; allow a bit of tolerance
        (is (nil? pulled)))))
  ; clean up
  (delete-feed "testqueue"))