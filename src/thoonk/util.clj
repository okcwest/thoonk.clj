(ns thoonk.util "Utility functions used everywhere by thoonk"
  (:require [taoensso.carmine :as redis])
  (:use [clojure.string :only [join]]))

(defn publish
  "Utility function to publish messages separated by null bytes.
   Second form allows supply a a separate pipeline to use for
   Redis communication"
  [key items]
  (redis/publish key (join "\00" items)))

(defn make-uuid
  []
  (str (java.util.UUID/randomUUID)))

