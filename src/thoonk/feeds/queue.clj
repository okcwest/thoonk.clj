(ns thoonk.feeds.queue
    (:use [thoonk.feeds.feed]
          [thoonk.redis-base]
          [clojure.tools.logging])
    (:require [taoensso.carmine :as redis]
              [thoonk.util :as util])
    (:import (thoonk.exceptions Empty)))

; a specific protocol for the queue feed type
(defprotocol QueueP
    ; queue-specific functions
    (push [this item] [this item priority])
    (pull [this] [this timeout]))

; this record contains top-level aliases for all inherited schemata
(defrecord Queue [name feed-ids feed-items feed-publish feed-publishes 
                  feed-retract feed-config feed-edit feed]
  QueueP ; implementations of queue-specific methods
    (push [this item priority]
        (let [id (util/make-uuid)]
          (with-redis-transaction
            (if priority
              (redis/rpush (:feed-ids this) id)
              (redis/lpush (:feed-ids this) id))
            (redis/hset (:feed-items this) id item)
            (redis/incr (:feed-publishes this)))
        id)) ; return the id we pushed!
      (push [this item]
          (push this item false))
    (pull [this timeout] ; blocking pull. waits for an item to appear.
        (let [result (with-redis (redis/brpop (:feed-ids this) timeout))]
            (if (or (nil? result) (= 0 (count result)))
                (throw (Empty.))
                (first ; first member of
                    (last ; all results at tail position
                      (let [id (nth result 1)]
                          (with-redis-transaction
                              (redis/hget (:feed-items this) id)
                              (redis/hdel (:feed-items this) id))))))))
    (pull [this] ; no timeout (wait forever)
        (pull this 0)))

(extend-protocol FeedP 
  ; override the following methods of FeedP when the 1st arg is a Queue
  Queue
    ; some queue-specific overrides of feed methods
    ; we alias publish to push, for queues.
    (publish 
      ([this item args]
        (if (nil? (:priority args))
          (push this item) ; default to non-priority
          (push this item (:priority args))))
      ([this item]
        (push this item false)))
    ; queue ids are managed with a list, not a sorted set, which means we 
    ; need to override all id manipulations.
    (get-ids [this]
      (with-redis (redis/lrange (:feed-ids this) 0 -1)))
    (retract [this id]
      ; first check if the id exists, which can't be done by a redis list:
      (let [ids (with-redis (redis/lrange (:feed-ids this) 0 -1))]
        (debug "Trying to remove id" id "from set of ids" ids)
        (if (some #{id} ids)
          ; found it.
          (do
            (debug "Found the id to remove")
          (with-redis-transaction
            (redis/lrem (:feed-ids this) 1 id)
            (redis/hdel (:feed-items this) id)
            (util/publish (:feed-retract this) id))))))
    (get-item 
      ; if no id specified, pull from the right and wait forever.
      ([this]
        (pull this))
      ([this id] ; when id is specified we can delegate to the feed.
        (get-item (:feed this) id)))
    ; we implement just about everything else by handing it off to the encapsulated feed
    (get-schemas [this]
      (get-schemas (:feed this)))
    (get-channels [this]
      (get-channels (:feed this)))
    (get-all [this]
      (get-all (:feed this))))


(defn make-queue [name]
  ; construct an encapsulated feed BY THE SAME NAME.
  ; this is safe coz no schemas conflict.
  (let [feed (make-feed name)]
    (Queue. name
            ; aliased from feed
            (:feed-ids feed) (:feed-items feed) (:feed-publish feed) 
            (:feed-publishes feed) (:feed-retract feed) (:feed-config feed) 
            (:feed-edit feed) 
            ; the feed object for delegating methods.
            feed)))
