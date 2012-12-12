(ns thoonk.core
  (:require [taoensso.carmine :as redis]
            [thoonk.util :as util])
  (:use [clojure.string :only [join split]]
        [clojure.tools.logging]
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

(declare initialize) ; registers the feed types. forward-declared for sanity.

;; UUID identifying this Thoonk JVM instance
(def uuid (str (java.util.UUID/randomUUID)))

(defn- parse-match [match]
  (let [msg (if (< 0 (count match)) (nth match 0) "")
        is-pattern (= "pmessage" msg)]
   ;(debug "Parsing match" match ": is-pattern = " is-pattern)
  { :msg msg
    :pattern (if (and is-pattern (< 1 (count match))) (nth match 1) nil)
    :channel  (if (and is-pattern (< 2 (count match))) 
              (nth match 2) 
              (if (< 1 (count match)) 
                (nth match 1) 
                ""))
    :data (if (and is-pattern (< 3 (count match))) 
              (nth match 3) 
              (if (< 2 (count match)) 
                (nth match 2) 
                ""))}))

; exception-handling wrapper for splitting
; potential for uncaught exceptions will apparently sink a handler.
(defn- safe-split [string pattern]
  (try (split string pattern)
    (catch Exception e (do 
      (error "Couldn't split" string ":" (.getMessage e))
      [string]))))

(defn create-listener
  "Defines and initializes a Thoonk listener instance for realtime feeds"
  []
  (let [handlers (atom {})
        emit (fn [event & args] (let [handler (get @handlers event)] (if (not (nil? handler)) (apply handler args))))]
      (-> (redis/with-new-pubsub-listener *redis-conn* ; this is setting up the listener map
            { "newfeed" (fn [match] 
                          (let [parsed (parse-match match)]
                            (if (not (= "message" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (first (safe-split (:data parsed) #"\000"))]
                                ;(debug "Channel match: channel" (:channel parsed) "got name" name)
                                (emit "create" name)))))
              "delfeed" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "message" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (first (safe-split (:data parsed) #"\000"))]
                                ;(debug "Channel match: channel" (:channel parsed) "got name" name)
                                (emit "delete" name)))))
              "conffeed" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "message" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (first (safe-split (:data parsed) #"\000"))]
                                ;(debug "Channel match: channel" (:channel parsed) "got name" name)
                                (emit "config" name)))))
              "feed.publish*" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "pmessage" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (last (safe-split (:channel parsed) #":"))
                                    data (safe-split (:data parsed) #"\000")
                                    id (if (< 0 (count data)) (nth data 0) nil)
                                    item (if (< 1 (count data)) (nth data 1) nil)]
                                (emit "publish" name item id)))))
              "feed.edit*" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "pmessage" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (last (safe-split (:channel parsed) #":"))
                                    data (safe-split (:data parsed) #"\000")
                                    id (if (< 0 (count data)) (nth data 0) nil)
                                    item (if (< 1 (count data)) (nth data 1) nil)]
                                (emit "edit" name item id)))))
              "feed.retract*" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "pmessage" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (last (safe-split (:channel parsed) #":"))
                                    id (:data parsed)]
                                (emit "retract" name id)))))
              "feed.position*" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "pmessage" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (last (safe-split (:channel parsed) #":"))
                                    data (safe-split (:data parsed) #"\000")
                                    id (if (< 0 (count data)) (nth data 0) nil)
                                    pos (if (< 1 (count data)) (nth data 1) nil)]
                                (emit "position" name id pos)))))
              "job.finish*" (fn [match]
                          (let [parsed (parse-match match)]
                            (if (not (= "pmessage" (:msg parsed)))
                              (info "Skipping traffic of type" (:msg parsed) "on channel" (:channel parsed))
                              (let [name (last (safe-split (:channel parsed) #":"))
                                    data (safe-split (:data parsed) #"\000")
                                    id (if (< 0 (count data)) (nth data 0) nil)
                                    result (if (< 1 (count data)) (nth data 1) nil)]
                                (emit "finish" name id result)))))
                            }
            (redis/subscribe "newfeed" "delfeed" "conffeed")
            (redis/psubscribe "feed.publish*" "feed.edit*" "feed.retract*" "feed.position*" "job.finish*"))
          (assoc :handlers handlers )))) ; attach the handlers to the resulting listener

(defn terminate-listener
  [listener]
  (redis/close-listener listener))

(defn register-handler
  [listener name handler]
  (swap! (:handlers listener) assoc name handler))
            
(defn remove-handler
  "Remove a function handler for a Thoonk event"
  [listener name]
  (swap! (:handlers listener) dissoc name))

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
      (if (nil? (get config "type"))
        (redis/hset (str "feed.config:" name) "type" "feed")))
    (if new
      (with-redis (util/publish "newfeed" [name uuid])))
    (with-redis (util/publish "conffeed" [name uuid]))))

(defn get-feed
  "Retrieves a feed instance from memory or constructs an instance if not cached"
  [name]
  (or (get @feeds name) ; try the cache first
    (let [feedtype (or ; what kind of feed should be constructed?
            (with-redis (redis/hget (str "feed.config:" name) "type")) 
            (throw (FeedDoesNotExist.))) ; die nicely if it isn't there.
          initialized (initialize) ; do this here before we try to get a c'tor
          feed-constructor (get @feedtypes feedtype)]
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
          (let [ config (or config {"type" feedtype})]
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
        (register-feedtype "feed" make-feed)
        (register-feedtype "sorted-feed" make-sorted-feed)
        (register-feedtype "queue" make-queue)
        (register-feedtype "job" make-job))))

