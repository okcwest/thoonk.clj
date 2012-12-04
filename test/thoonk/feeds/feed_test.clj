(ns thoonk.feeds.feed-test
  (:use clojure.test
    thoonk.core
    thoonk.feeds.feed
    thoonk.redis-base)
  (:require [taoensso.carmine :as redis]
            [thoonk.util :as util])
  (:import [thoonk.exceptions Empty]))

(deftest test-feed-schemas []
  (testing "Create a feed and check its schemata."
   (let [ created (create-feed "testfeed" {:type :feed})
          feed (get-feed "testfeed")
          schemas (get-schemas feed)]
    (is feed) ; verify we got something
    (is (contains? schemas "feed.ids:testfeed")) ; should all be there
    (is (contains? schemas "feed.items:testfeed"))
    (is (contains? schemas "feed.publish:testfeed"))
    (is (contains? schemas "feed.publishes:testfeed"))
    (is (contains? schemas "feed.retract:testfeed"))
    (is (contains? schemas "feed.config:testfeed"))
    (is (contains? schemas "feed.edit:testfeed"))
    (is (not (contains? schemas "feed.stalled:testfeed"))))) ; job only
  ; clean up
  (delete-feed "testfeed"))

(deftest test-feed-read-write []
  (testing "Put and get some items in a feed."
    (let [created (create-feed "testfeed" {:type :feed})
          feed (get-feed "testfeed")]
      (is (= [] (get-ids feed))) ; feed should start out empty
      (is (publish feed "testitem")) ; put an item
      (is (= 1 (count (get-ids feed)))) ; should now have one item
      (is (= "testitem" (get-item feed))); should get the same item back off
      (is (publish feed "seconditem")) ; put another
      (is (= 2 (count (get-ids feed)))) ; should have two now
      (let [allitems (get-all feed)] ; get-all fetches the expected items
        (is (= 2 (count (keys allitems))))
        (is (some #{"testitem"} (vals allitems)))
        (is (some #{"seconditem"} (vals allitems)))) ; no spurious contents by pigeonhole
      (is (= "testitem" (get-item feed))) ; FIFO
      (is (= "testitem" (get-item feed (first (get-ids feed))))) ; items by id
      (is (= "seconditem" (get-item feed (nth (get-ids feed) 1))))))
  ; clean up
  (delete-feed "testfeed"))

(deftest test-feed-retract []
  (testing "Retract an item and see that it becomes gone"
    (let [created (create-feed "testfeed" {:type :feed})
          feed (get-feed "testfeed")]
      (is (= [] (get-ids feed)))
      (is (publish feed "testitem"))
      (is (= 1 (count (get-ids feed)))) ; should now have one item
      (is (= "testitem" (get-item feed))); should get the same item back off
      (is (publish feed "seconditem")) ; put another
      (is (= 2 (count (get-ids feed)))) ; should have two now
      (let [id (first (get-ids feed))] ; get the id of the first one
        (is (= "testitem" (get-item feed id))) ; verify it's the one we expect
        (retract feed id) ; remove it
        (is (= 1 (count (get-ids feed)))) ; and then there was one
        (is (= "seconditem" (get-item feed))))
      (let [id (first (get-ids feed))] ; get the id of the remaining one
        (is (= "seconditem" (get-item feed id))) ; verify it's the one we expect
        (retract feed id) ; remove it
        (is (= 0 (count (get-ids feed)))) ; and then there was one
        (is (thrown? Empty (get-item feed))))))
    ; clean up
    (delete-feed "testfeed"))
