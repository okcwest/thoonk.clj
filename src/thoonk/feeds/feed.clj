(ns thoonk.feeds.feed
  (:require [taoensso.carmine :as redis])
  (:import (java.util UUID)))

(def pool         (redis/make-conn-pool)) ; See docstring for additional options
(def spec-server1 (redis/make-conn-spec)) ; ''
(defmacro wredis [& body] `(redis/with-conn pool spec-server1 ~@body))

(defprotocol FeedP
    (get-channels [])
    (delete-feed [])
    (get-schemas [])
    (get-ids [])
    (get-item [] [id])
    (get-all [])
    (publish [this item] [this item args])
    (retract [id]))


(defrecord Feed [name feed-ids feed-items feed-publish
                 feed-publishes feed-retract feed-config
                feed-edit]
    FeedP
        ; thoonk api
        (get-channels [this]
            [(:feed-publish this) (:feed-retract this) (:feed-edit this)])
        (get-schemas [this]
            #{(:feed-ids this) (:feed-items this) (:feed-publish this) 
                (:feed-publishes this) (:feed-retract this) (:feed-config this)
                (:feed-edit this)})
        ;(delete-feed [this]
            ; python client does this via the thoonk utility
            ;(thoonk/delete (:feed-ids this)))
        ; standard api
        (get-ids [this]
            (wredis (redis/zrange (:feed-ids this) 0 -1)))
        (get-item [this]
            (get-item this (wredis (redis/lindex (:feed-ids this) 0))))
        (get-item [this id]
            (wredis (redis/hget (:feed-items this) id)))
        (get-all [this]
            (wredis (redis/hgetall (:feed-items this))))
        (publish [this item args]
            (let [  id (:id args)
                    maxlen (hget pipe (:feed-config this) "max-length") 
                    delete-ids (if (or (nil? maxlen) (equal? 0 maxlen)) 
                        [] ; no stale items
                        (wredis (redis/zrange (:feed-ids this) 0 (- max 0))))]
                    (wredis ; pipeline the following together
                        (redis/multi) ; open a transaction block
                        (doseq [delete-id] delete-ids
                            (if (not (equal? delete-id id)) ; retract any stale items that were there
                                (redis/zrem (:feed-ids this) delete-id)
                                (redis/hget (:feed-items this) delete-id)
                                (thoonk/publish (:feed-retract this) delete-id)))
                        (redis/zadd (:feed-ids this) $TIME$) ; time-order the item's id
                        (redis/incr (:feed-publishes this))
                        (redis/hset (:feed-items this) id item) ; push the actual item
                        (redis/exec) ; commit the transaction
                    ))
            ))
        (publish [this item]
            (publish this (UUID/randomUUID)))
        (retract [this id]
            (if (not (nil? (wredis redis/zrank (:feed-ids this) id)))
                (wredis 
                    (redis/multi)
                    (redis/zrem (:feed-ids this) id)
                    (redis/hdel (:feed-items this) id)
                    (thoonk/publish (:feed-retract this) id)
                    (redis/exec))))

(defn make-feed
  [name]
  (Feed. name
         (str "feed.ids:" name)
         (str "feed.items:" name)
         (str "feed.publish:" name)
         (str "feed.publishes:" name)
         (str "feed.retract:" name)
         (str "feed.config:" name)
         (str "feed.edit:" name)))
