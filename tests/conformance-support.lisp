(in-package #:task-backend-sql/tests)

;;; 0.2.0 contract: the task-protocol 0.3.0 job-queue suite as a shared
;;; conformance suite, run against SQL-JOB-QUEUE on a live SQLite
;;; database. The conformance-*-test.lisp files are task-protocol
;;; tests/job-*-test.lisp (origin/main ba82dd5) with two changes only:
;;; %QUEUE below builds a SQL queue on a fresh in-memory database, and the
;;; tests that build their own in-memory journal build a SQL-TASK-JOURNAL
;;; on the queue's connection instead. Tests with no queue in them (the
;;; state table, the condition taxonomy) stay in task-protocol.
;;;
;;; Conventions (CL / Rove pitfalls this suite avoids), as in task-protocol:
;;; - Conditions are caught or handled OUTSIDE Rove's OK, whose own ERROR
;;;   handler would otherwise run first; OK only sees plain values.
;;; - Restarts are looked up and invoked INSIDE the handler, while the
;;;   signalling frame is live, by name without the condition argument
;;;   (a restart may be associated with another condition).
;;; - Worker threads record their conditions; nothing signals across a
;;;   thread boundary.

(defvar *test-connections* '()
  "Connections opened by %CONNECTION during the current test.")

(defvar *test-connections-lock* (bt:make-lock "test connections"))

(defun %connection (&optional (database-name ":memory:"))
  "A fresh SQLite connection, closed after the current test."
  (let ((c (sql-protocol:connect :driver :sqlite3
                                 :database-name database-name)))
    (bt:with-lock-held (*test-connections-lock*)
      (push c *test-connections*))
    c))

(defun %close-test-connections ()
  (let ((conns (bt:with-lock-held (*test-connections-lock*)
                 (shiftf *test-connections* '()))))
    (dolist (c conns)
      (ignore-errors (sql-protocol:disconnect c)))))

(defhook close-test-connections :after
  (%close-test-connections))

(defun %queue (&rest resource-classes)
  "A fresh SQL job queue on its own in-memory SQLite database;
   RESOURCE-CLASSES are (NAME LIMIT) lists."
  (task-backend-sql:make-sql-job-queue :connection (%connection)
                                       :resource-classes resource-classes))

(defun %catch (thunk)
  "The ERROR THUNK signals, or NIL when it returns normally."
  (handler-case (progn (funcall thunk) nil)
    (error (c) c)))

(defun %call-handling (type handler thunk)
  "Call THUNK with HANDLER (a function of the condition) run for every
   condition of TYPE. → (values THUNK's primary value, the list of TYPE
   conditions seen, oldest first)."
  (let ((seen '()))
    (values (handler-bind ((condition
                             (lambda (c)
                               (when (typep c type)
                                 (push c seen)
                                 (funcall handler c)))))
              (funcall thunk))
            (reverse seen))))

(defun %invoke (name &rest args)
  "Invoke the innermost restart named NAME with ARGS."
  (let ((r (find-restart name)))
    (if r
        (apply #'invoke-restart r args)
        (error "no ~s restart is active" name))))

(defun %restart-names (c)
  (mapcar #'restart-name (compute-restarts c)))

(defun %claimed (queue worker &rest keys)
  "CLAIM-JOB → the claimed job's id (NIL when nothing is claimed)."
  (let ((job (apply #'stack-task:claim-job queue worker keys)))
    (and job (stack-task:job-id job))))

(defun %state (queue id)
  (stack-task:job-state (stack-task:job-status queue id)))

(defun %event-seqs (events)
  (mapcar #'stack-task:event-seq events))

(defun %iota (n &optional (start 1))
  (loop for i from start below (+ start n) collect i))

(defun %transition-pairs (events)
  "(FROM TO) of each JOB-TRANSITION in EVENTS, in order."
  (loop for e in events
        when (stack-task:job-transition-p e)
          collect (list (stack-task:job-transition-from e)
                        (stack-task:job-transition-to e))))

(defun %run-threads (n fn &key (timeout 60))
  "Run (FN I) for I below N, each on its own thread; join them all.
   → the list of conditions the threads signalled (empty when clean).
   A thread still running after TIMEOUT seconds counts as an error."
  (let* ((lock (bt:make-lock "test threads"))
         (errors '())
         (threads
           (loop for i below n
                 collect (let ((i i))
                           (bt:make-thread
                            (lambda ()
                              (handler-case (funcall fn i)
                                (error (c)
                                  (bt:with-lock-held (lock)
                                    (push c errors)))))
                            :name (format nil "job-test-~d" i)))))
         (deadline (+ (get-internal-real-time)
                      (* timeout internal-time-units-per-second))))
    (dolist (th threads)
      (loop while (and (bt:thread-alive-p th)
                       (< (get-internal-real-time) deadline))
            do (sleep 0.01))
      (if (bt:thread-alive-p th)
          (push (make-condition 'simple-error
                                :format-control "thread ~a timed out"
                                :format-arguments (list (bt:thread-name th)))
                errors)
          (bt:join-thread th)))
    errors))

(defun %catch-value (thunk)
  "THUNK's primary value, or the ERROR it signals."
  (handler-case (funcall thunk)
    (error (c) c)))
