(ns thoonk.core
  (:require [taoensso.carmine :as redis]
            [thoonk.util :as util])
  (:use [clojure.string :only [join split]] 
        [thoonk.redis-base]
        [thoonk.feeds.feed]
        [thoonk.feeds.sorted-feed]
        [thoonk.feeds.queue]
        [thoonk.feeds.job])
  (:import (thoonk.exceptions FeedExists
                              FeedDoesNotExist
                              Empty
                              NotListening)))

;; Feed constructors
(def feedtypes (atom {}))

;; Cached feed instances
(def feeds (atom {}))

;(def base-schemas #{"feed.ids:" "feed.items:" "feed.publish:" "feed.publishes:" 
;  "feed.retract:" "feed.config:" "feed.edit:"})

;; we hold the set of schemas centrally, because it can't be pulled from the feed types
;(def feedtype-schemas (atom {
;    :feed base-schemas
;    :queue base-schemas
;    :sorted_feed (conj base-schemas #{"feed.incr_id:"})
;    :job (conj base-schemas #{"feed.claimed" "feed.stalled" "feed.running"
;      "feed.publishes" "feed.cancelled"})}))

;(defn get-schemas
;  "Fetches all the schemas for a feed by name"
;  [name]
;  (let [feedtype (get @feeds name)]
;    (if (nil? feedtype)
;      (throw (FeedDoesNotExist.)))
;    (loop [schemas #{} prefixes (get @feedtype-schemas feedtype)]
;      (if (or (nil? prefixes) (= 0(count prefixes)))
;        schemas
;        (recur (conj schemas (str (first prefixes))) (rest prefixes))))))

(declare initialize) ; registers the feed types. forward-declared for sanity.

;; UUID identifying this Thoonk JVM instance
(def uuid (str (java.util.UUID/randomUUID)))

(defmacro with-thoonk
  "Wrapping Thoonk functions in this macro rebinds the redis connection pool.
   See carmine/make-conn-pool and carmine/make-conn-spec for creating connection pools"
  [redis-pool redis-conn & body]
  (binding [*redis-pool* (or redis-pool *redis-pool*)
            *redis-conn* (or redis-conn *redis-conn*)]
    ~@body))

(defn create-listener
  "Defines and initializes a Thoonk listener instance for realtime feeds"
  []
  (let [handlers (atom {})
        emit (fn [event & args] (apply (event @handlers) args))]
    (with-redis
      (-> (redis/with-new-pubsub-listener *redis-conn*
            {"newfeed" (fn [msg channel data]
                         (let [[name] (split data #"\00")]
                           (emit "create" name)))
             "delfeed" (fn [msg channel data]
                         (let [[name] (split data #"\00")]
                           (emit "delete" name)))
             "conffeed" (fn [msg channel data]
                          (let [[name] (split data #"\00" 1)]
                            (emit "config" name)))
             "feed.publish*" (fn [msg _ channel data]
                               (let [[id item] (split data #"\00" 1)]
                                 (emit "publish" (last (split channel #":" 1) item id))))
             "feed.edit*" (fn [msg _ channel data]
                            (let [[id item] (split data #"\00" 1)]
                              (emit "edit" (last (split channel #":" 1)) item id)))
             "feed.retract*" (fn [msg _ channel data]
                               (emit "retract" (last (split channel #":" 1)) data))
             "feed.position*" (fn [msg _ channel data]
                                (let [[id rel-id] (split data #"\00" 1)]
                                  (emit "position" (last (split channel #":" 1)) id rel-id)))
             "job.finish*" (fn [msg _ channel data]
                             (let [[id result] (split data #"\00" 1)]
                               (emit "finish" (last (split channel #":" 1)) id result)))}
            (redis/subscribe "newfeed" "delfeed" "conffeed")
            (redis/psubscribe "feed.publish*" "feed.edit*" "feed.retract*" "feed.position*" "job.finish*"))
          ;; attach handlers to listener map
          (assoc :handlers handlers)))))

(defn terminate-listener
  [listener]
  nil)

(defn register-handler
  [listener name handler]
  (swap! (listener :handlers) assoc name handler))
            
(defn remove-handler
  "Remove a function handler for a Thoonk event"
  [listener name]
  (swap! (listener :handlers) dissoc name))

(defn feed-exists
  [name]
  (= 1 (with-redis
    (redis/sismember "feeds" name))))

(defn set-config
  "Sets the configuration values for a given feed"
  ([name config]
    (set-config name config false))
  ([name config new]
    (if (not (feed-exists name))
      (throw (FeedDoesNotExist.)))
    (with-redis
      (doseq [[key value] config]
        (redis/hset (str "feed.config:" name) key value))
      ; type must have a value, so default it to :feed
      (if (nil? (:type config))
        (redis/hset (str "feed.config:" name) :type :feed)))
    (if new
      (with-redis (util/publish "newfeed" [name uuid])))
    (with-redis (util/publish "conffeed" [name uuid]))))

(defn get-feed
  "Retrieves a feed instance from memory or constructs an instance if not cached"
  [name]
  (or (get @feeds name) ; try the cache first
    (let [feedtype (or ; what kind of feed should be constructed?
            (with-redis (redis/hget (str "feed.config:" name) :type)) 
            (throw (FeedDoesNotExist.))) ; die nicely if it isn't there.
          initialized (initialize) ; do this here before we try to get a c'tor
          feed-constructor (feedtype @feedtypes)]
      (if (not feed-constructor) ; unknown feed type? be cool.
          (throw (FeedDoesNotExist.))
          ; call the type-specific wrapper function that will create the type
          (get (swap! feeds assoc name (feed-constructor name feedtype)) name)))))

(defn create-feed
  "Creates keys for a new feed structure."
  [name config]
    (let [created (= 1 (with-redis (redis/sadd "feeds" name)))]
      (if created ; if this is new, set up the config keys so we know its type
        (set-config name config true))
      created))

(defn delete-feed
  "Deletes a feed's keys"
  [name]
  (with-redis
    (if (not (redis/sismember "feeds" name))
      (throw (FeedDoesNotExist.))))
  (let [feed-instance (get-feed name)
        schemas (get-schemas feed-instance)]
    (with-redis-transaction ;; inside transaction
      (redis/srem "feeds" name)
      (doseq [schema schemas] ; schemas never used won't exist and that's fine
        (redis/del schema))
      (util/publish "delfeed" [name uuid]))
      ; remove the deleted feed from the cache
      (swap! feeds dissoc name))
    ; if successful, getting the feed by name should break.
    (let [deleted-feed (try (get-feed name)
                          (catch FeedDoesNotExist f nil))]
      (nil? deleted-feed))) ; true if retrieve by name failed.

(defn register-feedtype
  "Registers a feed type by name for creation through Thoonk core"
  [feedtype type-constructor]
  ; wrap c'tor in an anonymous function that creates the redis keys if needed.
  (let [feed-constructor (fn [name config]
          (let [ config (or config {:type feedtype})]
            ; create the main key for the feed if needed
            (create-feed name config)
            ; keys for other schemas will be instantiated on first use.
            (type-constructor name)))]
    (swap! feedtypes assoc feedtype feed-constructor)))

(defn initialize 
  "Register the core Thoonk feed types"
  []
  (if (empty? @feedtypes)
      ; initialize the needed feed types.
      (do
        (register-feedtype :feed make-feed)
        (register-feedtype :sorted-feed make-sorted-feed)
        (register-feedtype :queue make-queue)
        (register-feedtype :job make-job))))

