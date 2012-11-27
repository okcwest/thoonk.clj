(ns thoonk.feeds.queue
    (:require [taoensso.carmine :as redis])
    (:use [thoonk.core]))

; a specific protocol for the queue feed type
(defprotocol QueueP
    ; queue-specific implementations
    (put [this item] [this item priority])
    (get [this] [this timeout])
    (get-ids [this]))

(defrecord Queue [name feed]
    FeedP
        ; we alias publish to put, for queues.
        (publish [this item]
            (publish this item false))
        ; we implement just about everything else by handing it off to the encapsulated feed
        (get-channels [this]
            (get-channels (:feed this)))
        (delete-feed [this]
            (delete-feed (:feed this)))
        (get-schemas [this]
            (get-schemas (:feed this)))
        (get-ids [this]
            (get-schemas (:feed this)))
        (get-item [this]
            (get-item (:feed this)))
        (get-item [this id]
            (get-item (:feed this) id))
        (get-all [this]
            (get-all (:feed this)))
        (publish [this item]
            (publish (:feed this) item)) 
        (publish [this item args]
            (if (nil? (:id args))
                (put (:feed this) item (:priority args)))
                (publish (:feed this) item (:id args)))
        (retract [this id]
            (retract (:feed this) id)))
        
    QueueP
        (publish [this item priority]
            (put this item priority))
        (put [this item priority]
            (let [id (make-uuid)]
                (with-redis-transaction
                    (if priority
                        (do
                            (redis/rpush (:feed-ids (:feed this)) id)
                            (redis/hset (:feed-items (:feed this)) id item)
                            (redis/incr (:feed-publishes (:feed this))))
                        (do
                            (redis/rpush (:feed-ids (:feed this)) id)
                            (redis/hset (:feed-items (:feed this)) id item)
                            (redis/incr (:feed-publishes (:feed this))))))))
        (put [this item]
            (put this item false))
        (get [this timeout]
            (let [result (with-redis (redis/brpop (:feed-ids (:feed this)) timeout))]
                (if (nil? result)
                    nil
                    (first
                        (let [id (nth 2 result)]
                            (with-redis-transaction
                                (redis/hget (:feed-items (:feed this)) id)
                                (redis/hdel (:feed-items (:feed this)) id)))))))
        (get [this]
            (get this 0))
        (get-ids [this]
            (with-redis (redis/lrange (:feed-ids (:feed this)) 0 -1))))
