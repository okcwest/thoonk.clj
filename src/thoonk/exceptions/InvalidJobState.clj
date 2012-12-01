(ns thoonk.exceptions.InvalidJobState
  (:gen-class :extends java.lang.Exception))

; thrown when we attempt an invalid job state transition.