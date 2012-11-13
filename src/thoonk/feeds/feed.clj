(ns thoonk.feeds.feed)

(defprotocol FeedP
    (get-channels [this])
    (delete-feed [this])
    (get-schemas [this])
    (get-ids [this])
    (get-item [this] [this id])
    (get-all [this])
    (publish [this item] [this item args])
    (retract [this id]))

