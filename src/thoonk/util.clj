(ns thoonk.util "Utility functions used everywhere by thoonk"
  (:require [taoensso.carmine :as redis])
  (:use [clojure.string :only [join]]))

(defn publish
  "Utility function to publish messages separated by null bytes.
   Second form allows supply a a separate pipeline to use for
   Redis communication"
  [key items]
  {:pre (vector? items)}
  (redis/publish key (join "\000" items)))

(defn make-uuid
  []
  (str (java.util.UUID/randomUUID)))

(defn get-time
  "Get the current time via a Date object."
  []
  (.getTime (java.util.Date.)))

(defn parse-int
  "Null-safe parsing of numeric strings"
  [number-string]
  (when-not (nil? number-string)
    (try (Integer/parseInt number-string)
      (catch NumberFormatException e nil))))
