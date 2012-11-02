(ns thoonk.feeds.feed)

(dfeprotocol FeedP
	(get-channels [])
	(delete-feed [])
	(get-schemas [])
	(get-ids [])
	(get-item [] [id])
	(get-all [])
	(publish [item] [item id])
	(retract [id]))
