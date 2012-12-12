(ns thoonk.feeds.job
    (:use [thoonk.feeds.feed]
          [thoonk.feeds.queue]
          [thoonk.redis-base]
          [clojure.set])
    (:require [taoensso.carmine :as redis]
              [thoonk.util :as util])
    (:import (thoonk.exceptions Empty
                                JobDoesNotExist
                                InvalidJobState)))

(defn- job-exists [job id]
  "private method to check if a job id refers to a valid item."
    (pos? (with-redis (redis/hexists (:feed-items job) id))))

; a specific protocol for the job feed type
(defprotocol JobP
    ; job-specific functions
    (get-failure-count [this id])
    (finish [this id] [this id result])
    (cancel [this id])
    (stall [this id])
    (retry [this id])
    (maintenance [this]))

; the job record wraps up a queue and dispatches some methods there.
(defrecord Job [name feed-ids feed-items feed-publish feed-publishes 
                feed-retract feed-config feed-edit feed-published 
                feed-cancelled feed-claimed feed-stalled feed-running 
                feed-finishes job-finish queue]
    JobP ; implementations of job-specific methods
      (get-failure-count [this id]
        (or ( util/parse-int (with-redis 
                              (redis/hget (:feed-cancelled this) id))) 
              0))

      (finish [this id]
        (finish this id nil))

      (finish [this id result]
        (if (not (job-exists this id))
          (throw (JobDoesNotExist.)))
        (if (nil? (with-redis (redis/zrank (:feed-claimed this) id)))
          (throw (InvalidJobState.)))
        (with-redis-transaction
          (redis/zrem (:feed-claimed this) id) ; a finished job is no longer claimed
          (redis/hdel (:feed-cancelled this) id) ; toss out the failure count
          (redis/zrem (:feed-published this) id) ; and depublish it.
          (if (not (nil? result)) ; don't publish nil result
            (util/publish (:job-finish this) [id result]))
          (redis/hdel (:feed-items this) id)))

      (cancel [this id]
        "Cancel the claimed job and immediately resubmit to the ready queue."
        (if (not (job-exists this id))
          (throw (JobDoesNotExist.)))
        (if (nil? (with-redis (redis/zrank (:feed-claimed this) id)))
          (throw (InvalidJobState.)))
        (with-redis-transaction
          (redis/hincrby (:feed-cancelled this) id 1) ; bump the failure count
          (redis/lpush (:feed-ids this) id) ; return to ready state, without priority
          (redis/zrem (:feed-claimed this) id))) ; no longer claimed

      (stall [this id]
        "Move a claimed task to a set of blocked tasks. Retry later to re-enqueue."
        (if (not (job-exists this id))
          (throw (JobDoesNotExist.)))
        (if (nil? (with-redis (redis/zrank (:feed-claimed this) id)))
          (throw (InvalidJobState.)))
        (with-redis-transaction
          (redis/zrem (:feed-claimed this) id) ; a stalled job is not claimed
          (redis/hdel (:feed-cancelled this) id) ; reset the failure count
          (redis/sadd (:feed-stalled this) id) ; add it to stalled jobs
          (redis/zrem (:feed-published this) id))) ; depublish it so nothing else picks it up.

      (retry [this id]
        "Resubmit a stalled job. Cancelled jobs are re-enqueued without this."
        (if (not (job-exists this id))
          (throw (JobDoesNotExist.)))
        (if (zero? (with-redis (redis/sismember (:feed-stalled this) id)))
          (throw (InvalidJobState.)))
        (let [result
                (with-redis-transaction
                  (redis/srem (:feed-stalled this) id) ; remove from stalled
                  (redis/lpush (:feed-ids this) id) ; return to ready queue
                  (let [score (.getTime (java.util.Date.))]
                    (redis/zadd (:feed-published this) score id)))] ; republish
          (= "OK" (first result))))

      (maintenance [this] 
        "Find jobs that are in no state and put them on the ready queue"
        ; returns count of fixed jobs.
        (let [result (with-redis-transaction
                (redis/hkeys (:feed-items this)) ; all items
                (redis/lrange (:feed-ids this) 0 -1) ; available items
                (redis/zrange (:feed-claimed this) 0 -1) ; claimed items
                (redis/smembers (:feed-stalled this))) ; stalled items
              all (nth (last result) 0)
              avail (nth (last result) 1)
              claimed (nth (last result) 2)
              stalled (nth (last result) 3)]
            (loop [added 0 ids all]
              (if (zero? (count ids))
                added) ; if no more ids to check, return the number added
              (let [id (first ids)
                    unaccounted (not (or (some #{id} avail) (some #{id} claimed) (some #{id} stalled)))]
                    (if unaccounted
                      (with-redis (redis/lpush (:feed-ids this) id)))
                    (recur (+ added (if (unaccounted) 1 0)) (rest ids)))))))

(extend-protocol QueueP
  ; override the following methods of QueueP when the 1st arg is a Job
  Job

    (push ; multiple-arity syntax differs between extend* and defrecord/deftype.
      ([this item]
        (push this item false))
      ([this item priority]
        (let [id (util/make-uuid)
              result (with-redis-transaction
                (if priority
                  (redis/rpush (:feed-ids this) id) ; pulls are from right
                  (redis/lpush (:feed-ids this) id))
                (redis/incr (:feed-publishes this))
                (redis/hset (:feed-items this) id item)
                (let [score (* 1000 (.getTime (java.util.Date.)))]
                  (redis/zadd (:feed-published this) score id)))]
          ; check the zadd to see if this was new.
          (if (pos? (last (last result)))
            (with-redis (util/publish (:feed-publishes this) [id item]))
            (with-redis (util/publish (:feed-edit this) [id item])))
          id))) ; return the id.

    (pull 
      ([this timeout]
        (let [next-item (with-redis (redis/brpop (:feed-ids this) timeout))
              id (if (or (nil? next-item) (zero? (count next-item)))
                (throw (Empty.))
                (last next-item)) ; brpop returns [key value]
              result (with-redis-transaction
                (redis/zadd (:feed-claimed this) (util/get-time) id) ; a job is claimed when we pull it.
                (redis/hget (:feed-items this) id)
                (redis/hget (:feed-cancelled this) id)
                (util/publish (:feed-claimed this) id))]
          { :id id
            :content (nth (last result) 1)
            :cancelled (or (util/parse-int (nth (last result) 2)) 0)}))
      ([this]
        (pull this 0))))

(extend-protocol FeedP 
  ; override the following methods of FeedP when the 1st arg is a Job
  Job
    ; some job-specific overrides of feed methods
    (get-channels [this]
      [(:feed-publishes this) (:feed-claimed this) (:feed-stalled this)
      (:feed-finishes this) (:feed-cancelled this) (:feed-retried this)
      (:job-finish this)])

    (get-schemas [this]
      ; need to add some to the standard batch
      (let [job-schemas #{(:feed-published this) (:feed-cancelled this)
              (:feed-claimed this) (:feed-stalled this) (:feed-running this)
              (:feed-finishes this) (:job-finish this)}]
          (union job-schemas (get-schemas (:queue this)))))

    ; go to the horse's mouth for ids
    (get-ids [this]
      (with-redis (redis/hkeys (:feed-items this))))

    ; alias publish to the Job version of push
    (publish
      ([this item args]
        (if (nil? (:priority args))
          (push this item) ; default to non-priority
          (push this item (:priority args))))
      ([this item]
        (push this item false)))

    (retract [this id] ; if the job id exists, yank it from all feeds.
      (if (with-redis (redis/hexists (:feed-items this) id))
        (with-redis-transaction
          (redis/hdel (:feed-items this) id)
          (redis/hdel (:feed-cancelled this) id)
          (redis/zrem (:feed-published this) id)
          (redis/srem (:feed-stalled this) id)
          (redis/zrem (:feed-claimed this) id)
          (redis/lrem (:feed-ids this) 1 id))))

    ; we delegate the rest of these.
    (get-item 
      ([this]
        ; with no id, pull from the right like a queue
        (pull (:queue this)))
      ([this id] ; when id is specified we handle it like a feed.
        (get-item (:feed (:queue this)) id)))

    (get-all [this]
      (get-all (:feed (:queue this)))))


(defn make-job [name]
  ; construct an encapsulated queue BY THE SAME NAME.
  ; this is safe coz no schemas conflict.
  (let [queue (make-queue name)]
    (Job. name
          ; aliased from queue (which aliases from its feed!)
          (:feed-ids queue) (:feed-items queue) (:feed-publish queue) 
          (:feed-publishes queue) (:feed-retract queue) (:feed-config queue) 
          (:feed-edit queue) 
          ; job-specific
          (str "feed.published:" name) (str "feed.cancelled:" name) 
          (str "feed.claimed:" name) (str "feed.stalled:" name) 
          (str "feed.running:" name) (str "feed.finishes:" name) 
          (str "job.finish:" name)
          ; the queue object for delegating methods
          queue)))
