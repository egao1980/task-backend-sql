(in-package #:task-backend-sql)

;;; 0.2.0 SQL job queue: the task-protocol 0.3.0 queue generics on
;;; sql-protocol. The semantics are task-protocol's (src/jobs.lisp header
;;; and generic-function docstrings); its in-memory backend
;;; (src/queue-memory.lisp) is the reference. Tests: tests/conformance-*.lisp
;;; run the task-protocol 0.3.0 queue suite against this class on a live
;;; SQLite database; tests/queue-sql-test.lisp adds what only SQL has.
;;;
;;; What this file adds to the protocol text
;;; ----------------------------------------
;;; Storage. Jobs live in task_job, resource classes in task_resource_class
;;; (queue-schema.lisp), events in the queue's SQL-TASK-JOURNAL
;;; (task_journal). The database is the only state that matters: a new
;;; SQL-JOB-QUEUE over the same database sees every job, class, lease and
;;; event, and continues the FIFO order and the "job-N" ids.
;;;
;;; Transactions. Every operation that changes rows runs in ONE database
;;; transaction: the state change and its JOB-TRANSITION event commit
;;; together or not at all. On SQLite the transaction starts with
;;; BEGIN IMMEDIATE (the write lock is taken first), so CLAIM-JOB's lease
;;; expiry, candidate scan, resource-class usage count, limit check and
;;; update are atomic against every other connection: under any number of
;;; connections and threads, a class's usage never exceeds its limit and
;;; no job is claimed twice. A condition that unwinds an operation (an
;;; unhandled error, a USE-VALUE that returns from it) rolls its
;;; transaction back. A CONTINUE restart that leaves the job unchanged
;;; commits nothing.
;;;
;;; Postgres (TODO). The claim there wants SELECT … FOR UPDATE SKIP LOCKED
;;; on the candidate plus a row lock on the class (FOR UPDATE on its
;;; task_resource_class row) before counting usage. Not implemented: the
;;; :POSTGRES dialect is refused at construction (TASK-ERROR). The
;;; worker-lease TODO in backend.lisp is the same item.
;;;
;;; Connections and threads. A queue owns one SQL-PROTOCOL connection and
;;; serializes its use with a recursive in-process lock, so one queue may
;;; be shared by threads. Several connections (several queue objects, or
;;; processes) over one SQLite file coordinate through the database
;;; locks; MAKE-SQL-JOB-QUEUE sets PRAGMA busy_timeout so a writer waits
;;; for the lock instead of failing at once.
;;;
;;; Subscriptions. Live delivery is in-process and per queue object.
;;; Events appended through this queue are delivered to its subscriptions
;;; after their transaction commits (a callback that reads the job, on
;;; this queue or another connection, sees the committed state), never
;;; inside an open transaction, in SEQ order, each once, no gaps (a
;;; subscription that missed events is pumped from the journal). The
;;; callback runs on the appending thread with the queue lock held (not
;;; the transaction), so it may call back into the queue. Events appended
;;; by another connection or process are not pushed: a subscriber there
;;; polls (JOB-EVENTS QUEUE ID :AFTER-SEQ LAST-SEEN), the pull form, and a
;;; new subscription's replay includes every event committed before it.
;;;
;;; Lease expiry is strict (LEASE-UNTIL < NOW), as in the in-memory
;;; backend and LEASE-CLAIMABLE-P. Values in KIND, PAYLOAD, RESULT and
;;; FAILURE go through CANONICALIZE-VALUE, then ENCODE-VALUE; a hash table
;;; has no column text and signals TASK-SERIALIZATION-ERROR (USE-VALUE).
;;;
;;; Status: the class and constructor are given; every queue method below
;;; is a stub (contract) for the implementer.

(defclass sql-job-queue (task-protocol:job-queue)
  ((connection :initarg :connection :reader sql-job-queue-connection
               :documentation "The SQL-PROTOCOL:SQL-CONNECTION it owns.")
   (dialect :initarg :dialect :reader sql-job-queue-dialect
            :initform :sqlite3
            :documentation ":SQLITE3 (the only one implemented).")
   (busy-timeout :initarg :busy-timeout :reader sql-job-queue-busy-timeout
                 :initform 5000
                 :documentation "Milliseconds a SQLite writer waits for the
                  database lock (PRAGMA busy_timeout).")
   (lock :initform (bt:make-recursive-lock "task-backend-sql job queue")
         :reader %sql-queue-lock
         :documentation "Serializes this object's use of CONNECTION.")
   (subscriptions :initform (make-hash-table :test 'equal)
                  :reader %sql-queue-subscriptions
                  :documentation "Job id → list of JOB-SUBSCRIPTIONs (this
                   object only)."))
  (:documentation "Job queue stored in SQL (see the file header). Private
   slots may be added by the implementation."))

(defun sql-job-queue-p (x)
  (typep x 'sql-job-queue))

(defmethod ensure-queue-schema ((queue sql-job-queue))
  (ensure-queue-schema (sql-job-queue-connection queue))
  queue)

(defun make-sql-job-queue (&key connection journal resource-classes
                             (ensure-schema t) (dialect :sqlite3)
                             (busy-timeout 5000))
  "A SQL-JOB-QUEUE over CONNECTION (a SQL-PROTOCOL:SQL-CONNECTION).
   JOURNAL defaults to a new SQL-TASK-JOURNAL on CONNECTION; a journal on
   another connection loses the one-transaction guarantee. DIALECT is
   :SQLITE3; anything else signals TASK-ERROR (Postgres is a TODO).
   On SQLite, sets PRAGMA busy_timeout = BUSY-TIMEOUT (ms). ENSURE-SCHEMA
   applies revisions 0001 and 0002 (ENSURE-QUEUE-SCHEMA). RESOURCE-CLASSES
   is a list of (NAME LIMIT), each passed to DEFINE-RESOURCE-CLASS in
   order; classes already in the database stay defined."
  (check-type connection sql-protocol:sql-connection)
  (check-type busy-timeout (integer 0))
  (unless (eq dialect :sqlite3)
    (error 'task-protocol:task-error
           :message (format nil "task-backend-sql 0.2.0: dialect ~s is not ~
                                 implemented (Postgres SKIP LOCKED claim is a TODO)"
                            dialect)))
  (sql-protocol:execute connection
                        (format nil "PRAGMA busy_timeout = ~d" busy-timeout))
  (when ensure-schema
    (ensure-queue-schema connection))
  (let ((queue (make-instance 'sql-job-queue
                              :connection connection
                              :journal (or journal
                                           (make-sql-task-journal
                                            :connection connection
                                            :ensure-schema nil))
                              :dialect dialect
                              :busy-timeout busy-timeout)))
    (dolist (entry resource-classes queue)
      (destructuring-bind (name limit) entry
        (task-protocol:define-resource-class queue name :limit limit)))))

;;; Stubs (contract). Each signals TASK-ERROR until implemented.

(defun %sql-queue-pending (name)
  (error 'task-protocol:task-error
         :message (format nil "task-backend-sql 0.2.0: ~(~a~) is not implemented yet"
                          name)))

(defmethod task-protocol:define-resource-class ((queue sql-job-queue) name
                                                &key limit)
  (declare (ignore name limit))
  (%sql-queue-pending 'define-resource-class))

(defmethod task-protocol:find-resource-class ((queue sql-job-queue) name)
  (declare (ignore name))
  (%sql-queue-pending 'find-resource-class))

(defmethod task-protocol:list-resource-classes ((queue sql-job-queue))
  (%sql-queue-pending 'list-resource-classes))

(defmethod task-protocol:resource-class-usage ((queue sql-job-queue) name)
  (declare (ignore name))
  (%sql-queue-pending 'resource-class-usage))

(defmethod task-protocol:submit-job ((queue sql-job-queue) kind
                                     &key payload priority resource-class now)
  (declare (ignore kind payload priority resource-class now))
  (%sql-queue-pending 'submit-job))

(defmethod task-protocol:job-status ((queue sql-job-queue) job-id)
  (declare (ignore job-id))
  (%sql-queue-pending 'job-status))

(defmethod task-protocol:list-jobs ((queue sql-job-queue)
                                    &key state kind resource-class)
  (declare (ignore state kind resource-class))
  (%sql-queue-pending 'list-jobs))

(defmethod task-protocol:cancel-job ((queue sql-job-queue) job-id
                                     &key reason force now)
  (declare (ignore job-id reason force now))
  (%sql-queue-pending 'cancel-job))

(defmethod task-protocol:cancel-requested-p ((queue sql-job-queue) job-id)
  (declare (ignore job-id))
  (%sql-queue-pending 'cancel-requested-p))

(defmethod task-protocol:claim-job ((queue sql-job-queue) worker-id
                                    &key kinds now lease-seconds)
  (declare (ignore worker-id kinds now lease-seconds))
  (%sql-queue-pending 'claim-job))

(defmethod task-protocol:heartbeat-job ((queue sql-job-queue) job-id worker-id
                                        &key now lease-seconds)
  (declare (ignore job-id worker-id now lease-seconds))
  (%sql-queue-pending 'heartbeat-job))

(defmethod task-protocol:complete-job ((queue sql-job-queue) job-id worker-id
                                       &key result now)
  (declare (ignore job-id worker-id result now))
  (%sql-queue-pending 'complete-job))

(defmethod task-protocol:fail-job ((queue sql-job-queue) job-id worker-id
                                   &key error now)
  (declare (ignore job-id worker-id error now))
  (%sql-queue-pending 'fail-job))

(defmethod task-protocol:acknowledge-cancel ((queue sql-job-queue) job-id
                                             worker-id &key now)
  (declare (ignore job-id worker-id now))
  (%sql-queue-pending 'acknowledge-cancel))

(defmethod task-protocol:expire-job-leases ((queue sql-job-queue) &key now)
  (declare (ignore now))
  (%sql-queue-pending 'expire-job-leases))

(defmethod task-protocol:append-job-event ((queue sql-job-queue) job-id event)
  (declare (ignore job-id event))
  (%sql-queue-pending 'append-job-event))

(defmethod task-protocol:report-job-progress ((queue sql-job-queue) job-id
                                              worker-id
                                              &key phase fraction message now)
  (declare (ignore job-id worker-id phase fraction message now))
  (%sql-queue-pending 'report-job-progress))

(defmethod task-protocol:job-events ((queue sql-job-queue) job-id
                                     &key after-seq)
  (declare (ignore job-id after-seq))
  (%sql-queue-pending 'job-events))

(defmethod task-protocol:subscribe-job-events ((queue sql-job-queue) job-id
                                               callback &key after-seq)
  (declare (ignore job-id callback after-seq))
  (%sql-queue-pending 'subscribe-job-events))

(defmethod task-protocol:unsubscribe-job-events ((queue sql-job-queue)
                                                 subscription)
  (declare (ignore subscription))
  (%sql-queue-pending 'unsubscribe-job-events))
