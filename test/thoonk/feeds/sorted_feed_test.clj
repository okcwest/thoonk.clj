(ns thoonk.feeds.sorted-feed-test
  (:use [clojure.test]
        [thoonk.core]
        [thoonk.feeds.feed]
        [thoonk.feeds.sorted-feed])
  (:require [thoonk.util :as util])
  (:import (thoonk.exceptions Empty
                              ItemDoesNotExist)))

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
      ; pull one off and make sure it is still sensible.
      (retract sfeed first-on)
      (is (= 5 (count (get-ids sfeed)))) ; right number of elements?
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