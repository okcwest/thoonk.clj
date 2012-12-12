(ns thoonk.redis-base
  (:require [taoensso.carmine :as redis]))

;; Default redis connection bindings
(def ^:dynamic *redis-pool* (redis/make-conn-pool))
(def ^:dynamic *redis-conn* (redis/make-conn-spec))

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

(defmacro with-thoonk
 "Wrapping Thoonk functions in this macro rebinds the redis connection pool.
  See carmine/make-conn-pool and carmine/make-conn-spec for creating connection pools"
 [redis-pool redis-conn & body]
 `(binding [*redis-pool* (or ~redis-pool *redis-pool*)
           *redis-conn* (or ~redis-conn *redis-conn*)]
   ~@body))



