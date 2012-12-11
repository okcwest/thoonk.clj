(ns thoonk.feeds.sorted-feed-test
  (:use [clojure.test]
        [thoonk.core]
        [thoonk.feeds.feed]
        [thoonk.feeds.sorted-feed]
        [clojure.tools.logging])
  (:require [thoonk.util :as util])
  (:import (thoonk.exceptions Empty
                              ItemDoesNotExist)))


; write some handlers to check the pubsub stuff in-line
(def handled (atom {})) ; for tracking handled events

(defn- handle-publish [name item id]
  (let [old (or (get @handled :published) [])]
    (debug "Item" item "with id" id "published to" name)
    (swap! handled assoc :published (conj old [name id item]))))

(defn- handle-retract [name id]
  (let [old (or (get @handled :retracted) [])]
    (debug "Item with id" id "retracted from" name)
    (swap! handled assoc :retracted (conj old [name id]))))

(defn- handle-position [name id pos]
  (let [old (or (get @handled :positioned) [])]
    (debug "Item with id" id "positioned at" pos "in" name)
    (swap! handled assoc :positioned (conj old [name id pos]))))

; fixture attaches the listener to every run
(defn listener-fixture [f]
  (let [listener (create-listener)]
    (register-handler listener "publish" handle-publish)
    (register-handler listener "retract" handle-retract)
    (register-handler listener "position" handle-position)
    (reset! handled {}) ; empty out the handled map
    (f) ; run the test function
    (terminate-listener listener)))

(use-fixtures :each listener-fixture)

(deftest test-sfeed-schemas []
  (testing "Make sure we can create a queue with all expected schemata"
    (let [created (create-feed "testsfeed" {:type :sorted-feed})
          sfeed (get-feed "testsfeed")
          schemas (get-schemas sfeed)]
     (is sfeed) ; verify we got something
     (is (contains? schemas "feed.ids:testsfeed")) ; should all be there
     (is (contains? schemas "feed.items:testsfeed"))
     (is (contains? schemas "feed.publish:testsfeed"))
     (is (contains? schemas "feed.publishes:testsfeed"))
     (is (contains? schemas "feed.retract:testsfeed"))
     (is (contains? schemas "feed.config:testsfeed"))
     (is (contains? schemas "feed.edit:testsfeed"))
     (is (contains? schemas "feed.idincr:testsfeed")) ; sfeed only
     (is (contains? schemas "feed.position:testsfeed"))
     (is (not (contains? schemas "feed.stalled:testqueue"))))) ; job only
  ; clean up
  (delete-feed "testsfeed"))

(deftest test-sfeed-publish-retract []
  (testing "Insert, check, and remove elements."
  (let [created (create-feed "testsfeed" {:type :sorted-feed})
        sfeed (get-feed "testsfeed")
        schemas (get-schemas sfeed)]
    (is sfeed) ; did we get a feed?
    ; add a bunch of elements to the feed.
    (let [first-on  (append sfeed "1st-on")
          ; add one to the front
          second-prepended (prepend sfeed "2nd-prepended")
          ; another on the back
          third-appended (append sfeed "3rd-appended")
          ; append is just an alias for publish
          fourth-appended (publish sfeed "4th-appended")
          fifth-inserted (publish-before 
                            sfeed fourth-appended "5th-inserted")
          sixth-inserted (publish-after 
                            sfeed second-prepended "6th-inserted")]
      (is (= 6 (count (get-ids sfeed)))) ; right number of elements?
      ; is the order what we are expecting?
      (is (= (map str [second-prepended sixth-inserted first-on third-appended 
              fifth-inserted fourth-appended]) 
            (get-ids sfeed)))
      ; all the right values?
      (is (= "1st-on" (get-item sfeed first-on)))
      (is (= "2nd-prepended" (get-item sfeed second-prepended)))
      (is (= "3rd-appended" (get-item sfeed third-appended)))
      (is (= "4th-appended" (get-item sfeed fourth-appended)))
      (is (= "5th-inserted" (get-item sfeed fifth-inserted)))
      (is (= "6th-inserted" (get-item sfeed sixth-inserted)))
      ; check that we registered publishes
      (Thread/sleep 500)
      (is (some #{["testsfeed" (str first-on) "1st-on"]} (get @handled :published)))
      (is (some #{["testsfeed" (str sixth-inserted) "6th-inserted"]} (get @handled :published)))
      ; see how we did on position handling
      (is (some #{["testsfeed" (str first-on) ":end"]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str second-prepended) "begin:"]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str fifth-inserted) (str ":" fourth-appended)]} (get @handled :positioned)))
      ; pull one off and make sure it is still sensible.
      (retract sfeed first-on)
      (is (= 5 (count (get-ids sfeed)))) ; right number of elements?
      (Thread/sleep 500)
      (is (some #{["testsfeed" (str first-on)]} (get @handled :retracted)))
      ; is the order what we are expecting?
      (is (= (map str [second-prepended sixth-inserted third-appended 
              fifth-inserted fourth-appended]) 
            (get-ids sfeed))))))
  ; clean up
  (delete-feed "testsfeed"))

(deftest test-sfeed-move-edit []
  (testing "Insert, check, and remove elements."
  (let [created (create-feed "testsfeed" {:type :sorted-feed})
        sfeed (get-feed "testsfeed")
        schemas (get-schemas sfeed)]
    (is sfeed) ; did we get a feed?
    ; add a bunch of elements to the feed.
    (let [one  (publish sfeed "one")
          two  (publish sfeed "two")
          three  (publish sfeed "three")
          four  (publish sfeed "four")]
      ; check that we got what's expected
      (is (= (map str [one two three four]) (get-ids sfeed)))
      ; move stuff around. use aliases first.
      (move-first sfeed three)
      (is (= (map str [three one two four]) (get-ids sfeed)))
      (move-last sfeed two)
      (is (= (map str [three one four two]) (get-ids sfeed)))
      (move-before sfeed four three)
      (is (= (map str [one three four two]) (get-ids sfeed)))
      (move-after sfeed one two)
      (is (= (map str [one two three four]) (get-ids sfeed)))
      (Thread/sleep 500) ; let event handling catch up
      (is (some #{["testsfeed" (str three) "begin:"]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str two) ":end"]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str three) (str ":" four)]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str two) (str one ":")]} (get @handled :positioned)))
      (is (some #{["testsfeed" (str three) "begin:"]} (get @handled :positioned)))
      ; some negative tests for the aliases
      ; move a non-existent id
      (is (thrown? ItemDoesNotExist (move-first sfeed "five")))
      (is (thrown? ItemDoesNotExist (move-before sfeed two "five")))
      ; move relative to a non-existent id
      (is (thrown? ItemDoesNotExist (move-after sfeed "five" two)))
      ; do the same operations by calling move directly.
      (move sfeed "begin:" three)
      (is (= (map str [three one two four]) (get-ids sfeed)))
      (move sfeed ":end" two)
      (is (= (map str [three one four two]) (get-ids sfeed)))
      (move sfeed (str ":" four) three)
      (is (= (map str [one three four two]) (get-ids sfeed)))
      (move sfeed (str one ":") two)
      (is (= (map str [one two three four]) (get-ids sfeed)))
      ; some more negative cases
      (is (thrown? IllegalArgumentException (move sfeed "invalid" three)))
      (is (thrown? ItemDoesNotExist (move sfeed "begin:" "five")))
      ; edit an item in place.
      (edit sfeed one "one and only")
      (is (= "one and only" (get-item sfeed one)))
      (is (thrown? ItemDoesNotExist (edit sfeed "five" "five-oh"))))))
  ; clean up
  (delete-feed "testsfeed"))