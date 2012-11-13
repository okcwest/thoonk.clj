(ns thoonk.feeds.feed-impl
  (:require [taoensso.carmine :as redis])
  (:use [thoonk.core]))

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
          (with-redis (redis/zrange (:feed-ids this) 0 -1)))
      (get-item [this]
          (get-item this (with-redis (redis/lindex (:feed-ids this) 0))))
      (get-item [this id]
          (with-redi (redis/hget (:feed-items this) id)))
      (get-all [this]
          (with-redis (redis/hgetall (:feed-items this))))
      (publish [this item args]
          (let [  id (:id args)
                  maxlen (hget pipe (:feed-config this) "max-length") 
                  delete-ids (if (or (nil? maxlen) (equal? 0 maxlen)) 
                      [] ; no stale items
                      (with-redis (redis/zrange (:feed-ids this) 0 (- max 0))))]
                  (with-redis-transaction
                      (doseq [delete-id] delete-ids
                          (if (not (equal? delete-id id)) ; retract any stale items that were there
                              (redis/zrem (:feed-ids this) delete-id)
                              (redis/hget (:feed-items this) delete-id)
                              (thoonk/publish (:feed-retract this) delete-id)))
                      (redis/zadd (:feed-ids this) $TIME$) ; time-order the item's id
                      (redis/incr (:feed-publishes this)) ; bump the counter
                      (redis/hset (:feed-items this) id item)))) ; push the actual item
      (publish [this item] ; if no id, make a uuid
          (publish this (make-uuid)))
      (retract [this id]
          (if (not (nil? (with-redis redis/zrank (:feed-ids this) id)))
              (with-redis-transaction 
                  (redis/zrem (:feed-ids this) id)
                  (redis/hdel (:feed-items this) id)
                  (thoonk/publish (:feed-retract this) id)))))

(defn ^Feed make-feed
    [name]
        (Feed. name
            (str "feed.ids:" name)
            (str "feed.items:" name)
            (str "feed.publish:" name)
            (str "feed.publishes:" name)
            (str "feed.retract:" name)
            (str "feed.config:" name)
            (str "feed.edit:" name)))

