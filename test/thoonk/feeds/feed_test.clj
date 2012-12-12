(ns thoonk.feeds.feed-test
  (:use clojure.test
    thoonk.core
    thoonk.feeds.feed
    thoonk.redis-base
    clojure.tools.logging)
  (:require [taoensso.carmine :as redis]
            [thoonk.util :as util])
  (:import [thoonk.exceptions Empty]))

; some handlers to check the pubsub stuff in-line
(def handled (atom {})) ; for tracking handled events

(defn- handle-publish [name item id]
  (let [old (or (get @handled :published) [])]
    (debug "Item" item "with id" id "published to" name)
    (swap! handled assoc :published (conj old [name id item]))))

(defn- handle-edit [name item id]
  (let [old (or (get @handled :edited) [])]
    (debug "Item" item "with id" id "edited in" name)
    (swap! handled assoc :edited (conj old [name id item]))))

(defn- handle-retract [name id]
  (let [old (or (get @handled :retracted) [])]
    (debug "Item with id" id "retracted from" name)
    (swap! handled assoc :retracted (conj old [name id]))))

; fixture attaches the listener to every run
(defn listener-fixture [f]
  (let [listener (create-listener)]
    (register-handler listener "publish" handle-publish)
    (register-handler listener "retract" handle-retract)
    (register-handler listener "edit" handle-edit)
    (reset! handled {}) ; empty out the handled map
    (f) ; run the test function
    (terminate-listener listener)))

(use-fixtures :each listener-fixture)

(deftest test-feed-schemas []
  (testing "Create a feed and check its schemata."
   (let [ created (create-feed "testfeed" {"type" "feed"})
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
    (let [created (create-feed "testfeed" {"type" "feed"})
          feed (get-feed "testfeed")]
      (is (= [] (get-ids feed))) ; feed should start out empty
      (let [first-id (publish feed "testitem")] ; put an item
        (is (= 1 (count (get-ids feed)))) ; should now have one item
        (Thread/sleep 500) ; let pubsub catch up
        (is (some #{["testfeed" first-id "testitem"]} (get @handled :published)))
        (is (= "testitem" (get-item feed))); should get the same item back off
        (let [second-id (publish feed "seconditem")]
          (is (= 2 (count (get-ids feed)))) ; should have two now
          (let [allitems (get-all feed)] ; get-all fetches the expected items
            (is (= 2 (count (keys allitems))))
            (is (= "testitem" (get allitems first-id)))
            (is (= "seconditem" (get allitems second-id)))
            (is (some #{"seconditem"} (vals allitems)))) ; no spurious contents by pigeonhole
          (is (= "testitem" (get-item feed))) ; FIFO
          (is (= "testitem" (get-item feed first-id))) ; items by id
          (is (= "seconditem" (get-item feed second-id)))
          ; now try to edit an item in place.
          (publish feed "secondedit" {:id second-id})
          ; can we see the change?
          (is (= "secondedit" (get-item feed second-id)))
          ; pubsub?
          (Thread/sleep 500)
          (is (some #{["testfeed" second-id "secondedit"]} (get @handled :edited)))
          (is (not (some #{["testfeed" second-id "secondedit"]} (get @handled :published))))))))
    ; clean up
    (delete-feed "testfeed"))

(deftest test-feed-retract []
  (testing "Retract an item and see that it becomes gone"
    (let [created (create-feed "testfeed" {"type" "feed"})
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
        (Thread/sleep 500)
        (is (some #{["testfeed" id]} (get @handled :retracted)))
        (is (= "seconditem" (get-item feed))))
      (let [id (first (get-ids feed))] ; get the id of the remaining one
        (is (= "seconditem" (get-item feed id))) ; verify it's the one we expect
        (retract feed id) ; remove it
        (is (= 0 (count (get-ids feed)))) ; and then there were none
        (is (thrown? Empty (get-item feed))))))
    ; clean up
    (delete-feed "testfeed"))
