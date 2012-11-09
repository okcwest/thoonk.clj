(ns thoonk.core
  (:require [taoensso.carmine :as redis])
  (:use [clojure.string :only [join split]])
  (:import (thoonk.exceptions FeedExists
                              FeedDoesNotExist
                              Empty
                              NotListening)))

;; Default redis connection bindings
(def ^:dynamic *redis-pool* (redis/make-conn-pool))
(def ^:dynamic *redis-conn* (redis/make-conn-spec))

;; Feed constructors
(def feedtypes (atom {}))

;; Cached feed instances
(def feeds (atom {}))

;; UUID identifying this Thoonk JVM instance
(defn make-uuid
  []
  (str (java.util.UUID/randomUUID)))
(def uuid (str (java.util.UUID/randomUUID)))

(defmacro with-thoonk
  "Wrapping Thoonk functions in this macro rebinds the redis connection pool.
   See carmine/make-conn-pool and carmine/make-conn-spec for creating connection pools"
  [redis-pool redis-conn & body]
  (binding [*redis-pool* (or redis-pool *redis-pool*)
            *redis-conn* (or redis-conn *redis-conn*)]
    ~@body))

(defmacro with-redis
  "Utility macro for in namespace redis use"
  [& body]
  `(redis/with-conn *redis-pool* *redis-conn* ~@body))

(defmacro with-redis-transaction
  [& body]
  `(redis/with-conn *redis-pool* *redis-conn*
     (redis/multi)
     ~@body
     (redis/exec)))

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

(defn- publish
  "Utility function to publish messages separated by null bytes.
   Second form allows supply a a separate pipeline to use for
   Redis communication"
  [key items]
  (redis/publish key (join "\00" items)))

(defn feed-exists
  [name]
  (with-redis
    (redis/sismember "feeds" name)))

(defn set-config
  "Sets the configuration values for a given feed"
  ([name config]
  (set-config name config false))
  ([name config new]
  (if (not (feed-exists name))
    (throw (FeedDoesNotExist.)))
  (let [feedtype (or (:type config) :feed)]
    (with-redis
      (doseq [[key value] config]
        (redis/hset (str "feed.config:" name) key value)))
    (if new
      (publish "newfeed" [name uuid]))
    (publish "conffeed" [name uuid]))))
      
(defn get-feed
  "Retrieves a feed instance from memory or constructs an instance if not cached"
  [name]
  (or (get @feeds name)
      (with-redis
        (let [feedtype (redis/hget (str "feed.config:" name) "type")
              feed-constructor (@feedtypes feedtype)]
          (if (not feedtype)
            (throw (FeedDoesNotExist.)))
          (swap! feeds assoc name (feed-constructor name))))))

(defn create-feed
  "Creates keys for a new feed structure"
  [name config]
  (with-redis
    (let [created (not (redis/sadd "feeds" name))]
      (if created
        (set-config name config true))
      created)))

(defn delete-feed
  "Deletes a feed's keys"
  [name]
  (with-redis
    (if (not (redis/sismember "feeds" name))
      (throw (FeedDoesNotExist.))))
  (with-redis ;; outside transaction
    (let [feed-instance (get-feed name)]
      (with-redis-transaction ;; inside transaction
        (redis/srem "feeds" name)
        (doseq [schema (get-schemas feed-instance)]
          (redis/delete schema))
        (publish "delfeed" [name uuid])))))

(defn register-feedtype
  "Registers a feed type by name for creation through Thoonk core"
  [feedtype type]
  (let [feed-constructor (fn [name config]
                           (let [config (or config {:type feedtype})]
                             (create-feed name config)))]
        (swap! feedtypes assoc feedtype feed-constructor)))