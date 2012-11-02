(ns thoonk.feeds.feed
  (:require [taoensso.carmine :as redis]))

(defprotocol FeedP
	(get-channels [])
	(delete-feed [])
	(get-schemas [])
	(get-ids [])
	(get-item [] [id])
	(get-all [])
	(publish [item] [item id])
	(retract [id]))

(defrecord Feed [name feed-ids feed-items feed-publish
                 feed-publishes feed-retract feed-config
                 feed-edit])

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

           

