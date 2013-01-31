(ns thoonk.feeds.job-test
  (:use [clojure.test]
        [thoonk.core]
        [thoonk.feeds.feed]
        [thoonk.feeds.job]
        [thoonk.feeds.queue]
        [clojure.tools.logging])
  (:require [thoonk.util :as util])
  (:import (thoonk.exceptions Empty
                              JobDoesNotExist
                              InvalidJobState)))

; write some handlers to check the pubsub stuff in-line
(def handled (atom {})) ; for tracking handled events

; a handler for each of the job channels
(defn- handle-publish [name item id]
  (let [old (or (get @handled :published) [])]
    (debug "Job" item "with id" id "published to" name)
    (swap! handled assoc :published (conj old [name id item]))))

; as thoonk stands, jobs can't actually be edited, but the channel exists.
;(defn- handle-edit [name item id]
;  (let [old (or (get @handled :edited) [])]
;    (debug "Job" item "with id" id "edited in" name)
;    (swap! handled assoc :edited (conj old [name id item]))))

; python pubsub ignores claim so we will too for now
;(defn- handle-claim [name id]
;  (let [old (or (get @handled :claimed) [])]
;    (debug "Job with id" id "claimed from" name)
;    (swap! handled assoc :claimed (conj old [name id]))))

(defn- handle-finish [name id result]
  (let [old (or (get @handled :finished) [])]
    (debug "Job with id" id "finished with result" result "in" name)
    (swap! handled assoc :finished (conj old [name id result]))))


; fixture attaches the listener to every run
(defn listener-fixture [f]
  (let [listener (create-listener)]
    (register-handler listener "publish" handle-publish)
;    (register-handler listener "edit" handle-edit)
;    (register-handler listener "claim" handle-claim)
    (register-handler listener "finish" handle-finish)
    (reset! handled {}) ; empty out the handled map
    (f) ; run the test function
    (terminate-listener listener)))

(use-fixtures :each listener-fixture)

(deftest test-job-schemas []
  (testing "Make sure we can create a queue with all expected schemata"
    (let [created (create-feed "testjob" {"type" "job"})
          job (get-feed "testjob")
          schemas (get-schemas job)]
     (is job) ; verify we got something
     (is (contains? schemas "feed.ids:testjob")) ; should all be there
     (is (contains? schemas "feed.items:testjob"))
     (is (contains? schemas "feed.publish:testjob"))
     (is (contains? schemas "feed.publishes:testjob"))
     (is (contains? schemas "feed.retract:testjob"))
     (is (contains? schemas "feed.config:testjob"))
     (is (contains? schemas "feed.edit:testjob"))
     (is (contains? schemas "feed.published:testjob"))
     (is (contains? schemas "feed.cancelled:testjob"))
     (is (contains? schemas "feed.claimed:testjob"))
     (is (contains? schemas "feed.stalled:testjob"))
     (is (contains? schemas "feed.running:testjob"))
     (is (contains? schemas "feed.finishes:testjob"))
     (is (contains? schemas "job.finish:testjob"))
     (is (not (contains? schemas "feed.position:testjob"))))) ;sorted feed only
  ; clean up
  (delete-feed "testjob"))

(deftest test-job-push-get-pull []
  (testing "Read and write jobs"
    (let [created (create-feed "testjob" {"type" "job"})
          job (get-feed "testjob")]
      (is job)
      (let [first-id (push job "first job content")
            second-id (push job"second job content")
            third-id (push job"third job content" true)]
        (is first-id)
        (is second-id)
        (is third-id)
        (is (= "first job content" (get-item job first-id)))
        (is (= "second job content" (get-item job second-id)))
        (is (= "third job content" (get-item job third-id)))
        ; be nice if an invalid id is requested.
        (is (nil? (get-item job "dne")))
        ; pick up the publish events
        (Thread/sleep 500)
        (is (some #{["testjob" (str first-id) "first job content"]} (get @handled :published)))
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 0} (pull job))) ; 3rd was pushed with priority
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 0} (pull job))) ; no more priority jobs so FIFO
        (is (= {:id second-id 
                :content "second job content" 
                :cancelled 0} (pull job))))))
  (delete-feed "testjob"))

(deftest test-job-stall-retry []
  (testing "Postpone and re-enable a job"
    (let [created (create-feed "testjob" {"type" "job"})
          job (get-feed "testjob")]
      (is job)
      (let [first-id (push job "first job content")
            second-id (push job "second job content")
            third-id (push job "third job content" true)]
        (is first-id)
        (is second-id)
        (is third-id)
        (is (= "first job content" (get-item job first-id)))
        (is (= "second job content" (get-item job second-id)))
        (is (= "third job content" (get-item job third-id)))
        ; pull, and get the 3rd priority job.
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 0} (pull job)))
        ; now go get the first job
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 0} (pull job)))
        ; move the third job into a stalled state.
        (is (stall job third-id))
        ; try to stall a non-existent job
        (is (thrown? JobDoesNotExist (stall job "dne")))
        ; try to stall the second job, which isn't claimed yet.
        (is (thrown? InvalidJobState (stall job second-id)))
        ; job 2 should be next, because job 3 is active but stalled.
        (is (= {:id second-id 
                :content "second job content" 
                :cancelled 0} (pull job)))
        ; pull again with a timeout, and expect nothing back.
        (let [t0 (util/get-time)]
          (is (thrown? Empty (pull job 1))) ; block while requesting
          (let [t1 (util/get-time)
                elapsed (- t1 t0)]
            (is (> 3000 elapsed 1000))))
        ; now, retry the stalled third job.
        (is (retry job third-id))
        ; retry some things that won't work, while we're at it
        (is (thrown? InvalidJobState (retry job third-id))) ; already done
        (is (thrown? InvalidJobState (retry job second-id))) ; not stalled
        (is (thrown? JobDoesNotExist (retry job "dne"))) ; no such animal
        ; on fetch, the cancellation count should still be 0.
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 0} (pull job))))))
  ; clean up
  (delete-feed "testjob"))

(deftest test-job-cancel []
  (testing "Cancel a job's execution attempt"
    (let [created (create-feed "testjob" {"type" "job"})
          job (get-feed "testjob")]
      (is job)
      (let [first-id (push job "first job content")
            second-id (push job"second job content")
            third-id (push job"third job content" true)]
        (is first-id)
        (is second-id)
        (is third-id)
        (is (= "first job content" (get-item job first-id)))
        (is (= "second job content" (get-item job second-id)))
        (is (= "third job content" (get-item job third-id)))
        ; pull, and get the 3rd priority job.
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 0} (pull job)))
        ; now go get the first job
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 0} (pull job)))
        ; cancel the third job. RESUBMITS WITHOUT PRIORITY.
        (is (cancel job third-id))
        ; try some things that won't work
        (is (thrown? InvalidJobState (cancel job second-id))) ; not running
        (is (thrown? JobDoesNotExist (cancel job "dne"))) ; not there
        ; check the failure count of the third job
        (is (= 1 (get-failure-count job third-id)))
        ; job 2 should be next, because job 3 was resubmitted to the back
        (is (= {:id second-id 
                :content "second job content" 
                :cancelled 0} (pull job)))
        ; pull again. the cancellation count should now be 1.
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 1} (pull job)))
        ; pull again with a timeout, and expect nothing back.
        (let [t0 (util/get-time)]
          (is (thrown? Empty (pull job 1))) ; block while requesting
          (let [t1 (util/get-time)
                elapsed (- t1 t0)]
            (is (> 3000 elapsed 1000)))))))
  ; clean up
  (delete-feed "testjob"))

(deftest test-job-finish []
  (testing "Complete a job and verify that it is gone"
    (let [created (create-feed "testjob" {"type" "job"})
          job (get-feed "testjob")]
      (is job)
      (let [first-id (push job "first job content")
            second-id (push job"second job content")
            third-id (push job"third job content" true)]
        (is first-id)
        (is second-id)
        (is third-id)
        (is (= "first job content" (get-item job first-id)))
        (is (= "second job content" (get-item job second-id)))
        (is (= "third job content" (get-item job third-id)))
        ; pull a job (should be the third)
        (is (= {:id third-id 
                :content "third job content" 
                :cancelled 0} (pull job)))
        ; finish the third job
        (is (finish job third-id))
        ; should not now be able to cancel, stall, or retry.
        (is (thrown? JobDoesNotExist (cancel job third-id)))
        (is (thrown? JobDoesNotExist (stall job third-id)))
        (is (thrown? JobDoesNotExist (retry job third-id)))
        ; try to finish the second job, which is not claimed yet.
        (is (thrown? InvalidJobState (finish job second-id)))
        ; pull another job, which should be the first.
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 0} (pull job)))
        ; stall, then try to finish
        (is (stall job first-id))
        (is (thrown? InvalidJobState (finish job first-id)))
        ; retry, then try to finish (will be re-enqueued at the back)
        (is (retry job first-id))
        (is (thrown? InvalidJobState (finish job first-id)))
        ; when we pull again, we should get the second job.
        (is (= {:id second-id 
                :content "second job content" 
                :cancelled 0} (pull job)))
        ; finish him!
        (is (finish job second-id))
        ; pull the first job.
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 0} (pull job)))
        ; cancel it and then try to finish it.
        (is (cancel job first-id))
        (is (thrown? InvalidJobState (finish job first-id)))
        ; pull it again (cancel count bumped)
        (is (= {:id first-id 
                :content "first job content" 
                :cancelled 1} (pull job)))
        ; should be able to finish now. publish this one with a result.
        (is (finish job first-id "result"))
        (is (some #{["testjob" (str first-id) "result"]} (get @handled :finished)))
        ; should be no more items on the job queue.
        (is (= 0 (count (get-all job)))))))
  ; clean up
  (delete-feed "testjob"))

  