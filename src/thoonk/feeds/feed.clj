(ns thoonk.feeds.feed
  (:use [thoonk.redis-base])
  (:require [taoensso.carmine :as redis]
            [thoonk.util :as util])
  (:import (thoonk.exceptions Empty)))

(defprotocol FeedP
  (get-channels [this])
  ;(delete-feed [this]) ; this is always done centrally.
  (get-schemas [this])
  (get-ids [this])
  (get-item [this] [this args])
  (get-all [this])
  (publish [this item] [this item args])
  (retract [this id]))

(defrecord Feed [ name feed-ids feed-items feed-publish
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
    ; standard api
    (get-ids [this]
      (with-redis (redis/zrange (:feed-ids this) 0 -1)))
    (get-item [this]
      (let [ids (with-redis (redis/zrange (:feed-ids this) 0 0))
            id (if (< 0 (count ids)) (first ids) (throw (Empty.)))]
        (get-item this id)))
    (get-item [this id]
      (with-redis (redis/hget (:feed-items this) id)))
    (get-all [this]
      (let [keyvalseq (with-redis (redis/hgetall (:feed-items this)))]
        (apply hash-map keyvalseq)))
    (publish
      [this item] ; if no id, make a uuid
        (publish this item {:id (util/make-uuid)}))
    (publish
      [this item args]
        (let [id (:id args)
              maxlen (with-redis (redis/hget (:feed-config this) "max-length")) 
              delete-ids (if (or (nil? maxlen) (= 0 maxlen)) 
                [] ; no stale items
                (with-redis (redis/zrange (:feed-ids this) 0 (- max 0))))]
          (with-redis-transaction
            (doseq [delete-id delete-ids]
              (if (not (= delete-id id)) ; retract any stale items that were there
                (do
                  (redis/zrem (:feed-ids this) delete-id)
                  (redis/hget (:feed-items this) delete-id)
                  (util/publish (:feed-retract this) delete-id))))
            (let [timestamp (.getTime (java.util.Date.))]
              ; WARNING: zadd's args are reversed! this is (zadd key score val)
              (redis/zadd (:feed-ids this) timestamp id)) ; time-order ids.
            (redis/incr (:feed-publishes this)) ; bump the counter
            (redis/hset (:feed-items this) id item)))) ; push the actual item
    (retract [this id]
      (if (not (nil? (with-redis (redis/zrank (:feed-ids this) id))))
        (with-redis-transaction
          (redis/zrem (:feed-ids this) id)
          (redis/hdel (:feed-items this) id)
          (util/publish (:feed-retract this) id)))))

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
