(ns thoonk.feeds.queue
    (:use [thoonk.feeds.feed]
          [thoonk.redis-base])
    (:require [taoensso.carmine :as redis]
              [thoonk.util :as util])
    (:import (thoonk.exceptions Empty)))

; a specific protocol for the queue feed type
(defprotocol QueueP
    ; queue-specific function
    (push [this item] [this item priority])
    (pull [this] [this timeout]))

(defrecord Queue [name feed]
    QueueP ; implementations of queue-specific methods
      (push [this item priority]
        (let [id (util/make-uuid)]
        (with-redis-transaction
          (if priority
            (do
              (redis/rpush (:feed-ids (:feed this)) id)
              (redis/hset (:feed-items (:feed this)) id item)
              (redis/incr (:feed-publishes (:feed this))))
            (do
              (redis/lpush (:feed-ids (:feed this)) id)
              (redis/hset (:feed-items (:feed this)) id item)
              (redis/incr (:feed-publishes (:feed this))))))))
        (push [this item]
            (push this item false))
        (pull [this timeout] ; blocking pull. waits for an item to appear.
            (let [result (with-redis (redis/brpop (:feed-ids (:feed this)) timeout))]
                (if (or (nil? result) (= 0 (count result)))
                    (throw (Empty.))
                    (first ; first member of
                        (last ; all results at tail position
                          (let [id (nth result 1)]
                              (with-redis-transaction
                                  (redis/hget (:feed-items (:feed this)) id)
                                  (redis/hdel (:feed-items (:feed this)) id))))))))
        (pull [this] ; no timeout (wait forever)
            (pull this 0)))

(extend-protocol FeedP 
  ; override the following methods of FeedP when the 1st arg is a Queue
  Queue
    ; some queue-specific overrides of feed methods
    ; we alias publish to push, for queues.
    (publish [this item]
      (publish this item false))
    (publish [this item args]
      (if (nil? (:priority args))
        (push this item) ; default to non-priority
        (push this item (:priority args))))
    ; queue ids are managed with a list, not a sorted set, which means we 
    ; need to override all id manipulations.
    (get-ids [this]
      (with-redis (redis/lrange (:feed-ids (:feed this)) 0 -1)))
    (retract [this id]
      ; first check if the id exists, which can't be done by a redis list:
      (let [ids (with-redis (redis/lrange (:feed-ids this) 0 -1))]
        (if (some (fn [a] (= id a)) ids)
          ; found it.
          (with-redis-transaction
            (redis/zrem (:feed-ids this) id)
            (redis/hdel (:feed-items this) id)
            (util/publish (:feed-retract this) id)))))
    ; if no id specified, pull from the right and wait forever.
    (get-item [this]
      (pull this 0))
    ; we implement just about everything else by handing it off to the encapsulated feed
    (get-item [this id] ; when id is specified we can handle it like the feed.
      (get-item (:feed this) id))
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
    (Queue. name feed)))
