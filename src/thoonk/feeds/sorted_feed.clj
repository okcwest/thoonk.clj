(ns thoonk.feeds.sorted-feed
    (:use [clojure.set]
          [thoonk.feeds.feed]
          [thoonk.redis-base])
    (:require [taoensso.carmine :as redis]
              [thoonk.util :as util])
    (:import (thoonk.exceptions Empty
                                ItemDoesNotExist)))

(defn- item-exists [sfeed id]
    (< 0 (with-redis (redis/hexists (:feed-items sfeed) id))))

(defn- insert [sfeed item rel-id method]
  "private function for inserting items"
  (if (not (item-exists sfeed rel-id))
    (throw (ItemDoesNotExist.)))
  (let [id (with-redis (redis/incr (:feed-id-incr sfeed)))
        pos-rel-id (if (= "BEFORE" method)
                      (str ":" rel-id)
                      (str rel-id ":"))]
      (with-redis-transaction
        (redis/linsert (:feed-ids sfeed) method rel-id id)
        (redis/hset (:feed-items sfeed) id item)
        (util/publish (:feed-publish sfeed) [id item])
        (util/publish (:feed-position sfeed) [id pos-rel-id]))
    id))

; a specific protocol for the sorted-feed type
(defprotocol SortedFeedP
    ; sfeed-specific functions
    (append [this item])
    (prepend [this item])
    (edit [this id item])
    ; convenience functions for calling (insert)
    (publish-before [this before-id item])
    (publish-after [this after-id item])
    (move [this pos id])
    ; convenience functions for calling (move)
    (move-before [this rel-id id])
    (move-after [this rel-id id])
    (move-first [this id])
    (move-last [this id]))
    

; this record contains top-level aliases for all inherited schemata
(defrecord SortedFeed [ name
                        ; inherit these from feed
                        feed-ids feed-items feed-publish feed-publishes 
                        feed-retract feed-config feed-edit 
                        ; sorted-feed specific
                        feed-id-incr feed-position
                        feed]
  SortedFeedP ; implementations of sfeed-specific methods
    (append [this item] ; delegate to publish
      (publish this item))
    (prepend [this item]
      (let [id (with-redis (redis/incr (:feed-id-incr this)))]
      (with-redis-transaction
        (redis/lpush (:feed-ids this) id)
        (redis/incr (:feed-publishes this))
        (redis/hset (:feed-items this) id item)
        (util/publish (:feed-publish this) [id item])
        (util/publish (:feed-position this) [id "begin:"]))
      id))
    (edit [this id item]
      (if (not (item-exists this id))
        (throw (ItemDoesNotExist.)))
      (with-redis-transaction
        (redis/hset (:feed-items this) id item)
        (redis/incr (:feed-publishes this))
        (util/publish (:feed-publish this) [id item])))
    (publish-before [this rel-id item]
      (insert this item rel-id "BEFORE"))
    (publish-after [this rel-id item]
      (insert this item rel-id "AFTER"))
    (move [this pos id]
      ;"Move item with this id to the specified position.
      ;:42 means before item with id 42.
      ;42: means after it.
      ;begin: is the beginning and :end is the end."
      (let [op (if (= \: (first pos)) 
                  "BEFORE" 
                  (if (= \: (last pos)) 
                    "AFTER" 
                    (throw (IllegalArgumentException. 
                            (str pos " is not a valid position argument.")))))
            rel-id (if (= "BEFORE" op) 
                      (subs pos 1 (count pos))
                      (subs pos 0 (- (count pos) 1)))]
        (if (not (item-exists this id))
          (throw (ItemDoesNotExist.)))
        (if (not (or  (= "begin" rel-id) 
                      (= "end" rel-id) 
                      (item-exists this rel-id)))
          (throw (ItemDoesNotExist. 
                  (str "Relative id '" rel-id 
                    "' is not an item id, 'end', or 'begin'."))))
        (with-redis-transaction
          (redis/lrem (:feed-ids this) 1 id) ; remove from old position
          (if (= "begin" rel-id)
            (redis/lpush (:feed-ids this) id)
            (if (= "end" rel-id)
              (redis/rpush (:feed-ids this) id)
              (redis/linsert (:feed-ids this) op rel-id id)))
          (util/publish (:feed-position this) [id pos]))))
    (move-before [this rel-id id]
      (move this (str ":" rel-id) id))
    (move-after [this rel-id id]
      (move this (str rel-id ":") id))
    (move-first [this id]
      (move this "begin:" id))
    (move-last [this id]
      (move this ":end" id)))

(extend-protocol FeedP 
  ; override the following methods of FeedP when the 1st arg is a SortedFeed
  SortedFeed
    ; some sfeed-specific overrides of feed methods
    (get-channels [this]
      [(:feed-publish this) (:feed-retract this) (:feed-position this)])
    (get-schemas [this]
      ; need to add some to the standard batch
      (let [sfeed-schemas #{(:feed-id-incr this) (:feed-position this)}]
          (union sfeed-schemas (get-schemas (:feed this)))))
    (publish 
      ([this item args] ; throw away args when passed to a sorted feed.
        (publish (:feed this) item))
      ([this item]
        (let [id (with-redis (redis/incr (:feed-id-incr this)))]
          (with-redis-transaction
            (redis/rpush (:feed-ids this) id)
            (redis/incr (:feed-publishes this))
            (redis/hset (:feed-items this) id item)
            (util/publish (:feed-publish this) [id item])
            (util/publish (:feed-position this) [id ":end"]))
          id))) ; return the published id
    (retract [this id]
      (if (item-exists this id)
        (with-redis-transaction
          (redis/lrem (:feed-ids this) 1 id)
          (redis/hdel (:feed-items this) id)
          (util/publish (:feed-retract this) [id]))))
          
    (get-ids [this]
      (with-redis (redis/lrange (:feed-ids this) 0 -1)))
    (get-item
      ([this]
        ; if no id specified, pull from the right
        (get-item this (last (get-ids this))))
      ([this id]
        ; 2-ary delegates to feed.
        (get-item (:feed this) id)))
    ; we delegate this to the encapsulated feed
    (get-all [this]
      (get-all (:feed this))))


(defn make-sorted-feed [name]
  ; construct an encapsulated feed BY THE SAME NAME.
  ; this is safe coz no schemas conflict.
  (let [feed (make-feed name)]
    (SortedFeed. name
            ; aliased from feed
            (:feed-ids feed) (:feed-items feed) (:feed-publish feed) 
            (:feed-publishes feed) (:feed-retract feed) (:feed-config feed) 
            (:feed-edit feed)
            ; feed-specific
            (str "feed.idincr:" name) (str "feed.position:" name)
            ; the feed object for delegating methods.
            feed)))
