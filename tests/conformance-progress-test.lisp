(in-package #:task-backend-sql/tests)

;;; 0.2.0 conformance: task-protocol 0.3.0 tests/job-progress-test.lisp against
;;; SQL-JOB-QUEUE (see conformance-support.lisp). progress events, ordering, subscription and replay.

(defun %running-job (q &key (worker "w") (now 100))
  "Submit one job and claim it. → its id; journal seqs 1 and 2 are used."
  (let ((id (stack-task:submit-job q :k :now (1- now))))
    (stack-task:claim-job q worker :now now :lease-seconds 3600)
    id))

(defun %collector ()
  "→ (values callback reader): CALLBACK pushes events; (READER) lists them
   oldest first."
  (let ((events '()))
    (values (lambda (e) (push e events))
            (lambda () (reverse events)))))

(deftest report-progress-appends-in-order
  (let* ((q (%queue))
         (id (%running-job q))
         (events (loop for i from 1 to 3
                       collect (stack-task:report-job-progress
                                q id "w" :phase "step" :fraction (/ i 4)
                                :message (format nil "~d of 4" i)
                                :now (+ 200 i)))))
    (ok (every #'stack-task:job-progress-p events))
    (ok (equal '(3 4 5) (%event-seqs events)))
    (ok (equal '(201 202 203) (mapcar #'stack-task:event-timestamp events)))
    (ok (equal '(1/4 1/2 3/4) (mapcar #'stack-task:job-progress-fraction events)))
    (ok (equal "w" (stack-task:job-progress-worker-id (first events))))
    (ok (equal '(1 2 3 4 5) (%event-seqs (stack-task:job-events q id))))
    (ok (equal '(4 5) (%event-seqs (stack-task:job-events q id :after-seq 3))))
    (ok (null (stack-task:job-events q id :after-seq 5)))
    (ok (equal '("1 of 4" "2 of 4" "3 of 4")
               (mapcar #'stack-task:job-progress-message
                       (remove-if-not #'stack-task:job-progress-p
                                      (stack-task:job-events q id)))))))

(deftest progress-fraction-bounds
  (let* ((q (%queue))
         (id (%running-job q)))
    (dolist (f (list nil 0 1 1/2 0.25d0))
      (ok (stack-task:job-progress-p
           (stack-task:report-job-progress q id "w" :fraction f))))
    (dolist (f (list 1.5 -0.1 2 "half" :done))
      (ok (typep (%catch (lambda ()
                           (stack-task:report-job-progress q id "w" :fraction f)))
                 'stack-task:job-progress-error)))
    (ok (= (+ 2 5) (length (stack-task:job-events q id))))))

(deftest progress-does-not-extend-lease
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 100 :lease-seconds 10)
    (stack-task:report-job-progress q id "w" :fraction 1/2 :now 105)
    (ok (eql 110 (stack-task:job-lease-until (stack-task:job-status q id))))))

(deftest progress-requires-running-holder
  (let* ((q (%queue))
         (running (%running-job q :worker "w1"))
         (queued (stack-task:submit-job q :k :now 200)))
    (let ((c (%catch (lambda ()
                       (stack-task:report-job-progress q queued "w1" :fraction 0)))))
      (ok (typep c 'stack-task:job-state-error))
      (ok (not (typep c 'stack-task:job-lease-lost)))
      (ok (eq :queued (and c (stack-task:job-state-error-state c))))
      (ok (eq :progress (and c (stack-task:job-state-error-operation c)))))
    (ok (typep (%catch (lambda ()
                         (stack-task:report-job-progress q running "w2" :fraction 0)))
               'stack-task:job-lease-lost))
    (ok (= 1 (length (stack-task:job-events q queued))))))

(deftest subscription-replays-then-goes-live
  (let* ((q (%queue))
         (id (%running-job q)))
    (stack-task:report-job-progress q id "w" :phase "a")
    (stack-task:report-job-progress q id "w" :phase "b")
    (multiple-value-bind (callback seen) (%collector)
      (let ((sub (stack-task:subscribe-job-events q id callback)))
        (ok (typep sub 'stack-task:job-subscription))
        (ok (equal id (stack-task:job-subscription-job-id sub)))
        (ok (stack-task:job-subscription-active-p sub))
        (ok (equal '(1 2 3 4) (%event-seqs (funcall seen))))
        (ok (eql 4 (stack-task:job-subscription-last-seq sub)))
        (stack-task:report-job-progress q id "w" :phase "c")
        (stack-task:complete-job q id "w" :result :ok)
        (ok (equal '(1 2 3 4 5 6) (%event-seqs (funcall seen))))
        (ok (eql 6 (stack-task:job-subscription-last-seq sub)))
        (ok (equal '(:job-transition :job-transition :job-progress
                     :job-progress :job-progress :job-transition)
                   (mapcar #'stack-task:event-type-keyword (funcall seen))))
        (ok (equal '("a" "b" "c")
                   (loop for e in (funcall seen)
                         when (stack-task:job-progress-p e)
                           collect (stack-task:job-progress-phase e))))))))

(deftest subscription-reconnect-replays-missed
  (let* ((q (%queue))
         (id (%running-job q)))
    (multiple-value-bind (cb1 seen1) (%collector)
      (let ((sub1 (stack-task:subscribe-job-events q id cb1)))
        (stack-task:report-job-progress q id "w" :phase "a")
        (stack-task:unsubscribe-job-events q sub1)
        (ok (not (stack-task:job-subscription-active-p sub1)))
        (stack-task:unsubscribe-job-events q sub1)
        (stack-task:report-job-progress q id "w" :phase "b")
        (stack-task:report-job-progress q id "w" :phase "c")
        (ok (equal '(1 2 3) (%event-seqs (funcall seen1))))
        (multiple-value-bind (cb2 seen2) (%collector)
          (stack-task:subscribe-job-events
           q id cb2 :after-seq (stack-task:job-subscription-last-seq sub1))
          (ok (equal '(4 5) (%event-seqs (funcall seen2))))
          (stack-task:report-job-progress q id "w" :phase "d")
          (ok (equal '(4 5 6) (%event-seqs (funcall seen2))))
          (ok (equal '(1 2 3) (%event-seqs (funcall seen1))))
          (ok (equal (%iota 6)
                     (%event-seqs (append (funcall seen1) (funcall seen2))))))))))

(deftest subscription-after-seq-past-end
  (let* ((q (%queue))
         (id (%running-job q)))
    (multiple-value-bind (cb seen) (%collector)
      (let ((sub (stack-task:subscribe-job-events q id cb :after-seq 2)))
        (ok (null (funcall seen)))
        (ok (eql 2 (stack-task:job-subscription-last-seq sub)))
        (stack-task:report-job-progress q id "w" :phase "x")
        (ok (equal '(3) (%event-seqs (funcall seen))))))))

(deftest subscriptions-are-per-job
  (let* ((q (%queue))
         (a (%running-job q))
         (b (%running-job q)))
    (multiple-value-bind (cb seen) (%collector)
      (stack-task:subscribe-job-events q a cb)
      (stack-task:report-job-progress q b "w" :phase "b")
      (stack-task:report-job-progress q a "w" :phase "a")
      (ok (every (lambda (e) (equal a (stack-task:event-task-id e)))
                 (funcall seen)))
      (ok (equal '(1 2 3) (%event-seqs (funcall seen)))))))

(deftest append-job-event-custom
  (let* ((q (%queue))
         (id (%running-job q)))
    (multiple-value-bind (cb seen) (%collector)
      (stack-task:subscribe-job-events q id cb :after-seq 2)
      (let ((ev (stack-task:append-job-event
                 q id (make-instance 'stack-task:wait-input :prompt "approve?"))))
        (ok (equal id (stack-task:event-task-id ev)))
        (ok (eql 3 (stack-task:event-seq ev)))
        (ok (equal '(3) (%event-seqs (funcall seen))))
        (ok (eql 3 (length (stack-task:journal-events
                            (stack-task:job-queue-journal q) id))))))))

(deftest subscription-ordered-under-concurrent-appends
  (let* ((q (%queue))
         (id (%running-job q))
         (per-thread 25)
         (threads 4)
         (lock (bt:make-lock "subscriber"))
         (seen '())
         (go-flag nil))
    (let* ((sub nil)
           (errors
             (%run-threads
              (1+ threads)
              (lambda (i)
                (if (= i threads)
                    ;; Subscribe from its own thread while the others append.
                    (progn
                      (loop until go-flag do (sleep 0.0005))
                      (sleep 0.002)
                      (setf sub (stack-task:subscribe-job-events
                                 q id
                                 (lambda (e)
                                   (bt:with-lock-held (lock)
                                     (push (stack-task:event-seq e) seen))))))
                    (progn
                      (setf go-flag t)
                      (dotimes (k per-thread)
                        (stack-task:report-job-progress
                         q id "w" :phase (format nil "t~d" i)
                         :fraction (/ k per-thread)))))))))
      (ok (null errors))
      (ok (typep sub 'stack-task:job-subscription))
      (let ((total (+ 2 (* threads per-thread))))
        (ok (equal (%iota total) (reverse seen)))
        (ok (equal (%iota total)
                   (%event-seqs (stack-task:job-events q id))))))))

(deftest subscriber-error-continue-skips-delivery
  (let* ((q (%queue))
         (id (%running-job q))
         (delivered '()))
    (stack-task:subscribe-job-events
     q id
     (lambda (e)
       (when (and (stack-task:job-progress-p e)
                  (equal "bad" (stack-task:job-progress-phase e)))
         (error "subscriber broke"))
       (push (stack-task:event-seq e) delivered))
     :after-seq 2)
    (multiple-value-bind (ev seen)
        (%call-handling 'simple-error
                        (lambda (c) (declare (ignore c)) (%invoke 'continue))
                        (lambda ()
                          (stack-task:report-job-progress q id "w" :phase "bad")))
      (ok (= 1 (length seen)))
      (ok (stack-task:job-progress-p ev))
      (ok (eql 3 (and ev (stack-task:event-seq ev)))))
    (stack-task:report-job-progress q id "w" :phase "good")
    (ok (equal '(4) (reverse delivered)))
    (ok (equal '(1 2 3 4) (%event-seqs (stack-task:job-events q id))))
    (ok (typep (%catch (lambda ()
                         (stack-task:report-job-progress q id "w" :phase "bad")))
               'simple-error))))
