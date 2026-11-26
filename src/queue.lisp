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
;;; Status: implemented (0.2.0). BEGIN IMMEDIATE is sent as SQL until
;;; sql-protocol#12 (with-transaction :mode); see sql-protocol#13 for busy errors.

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
                   object only).")
   (in-transaction :initform nil :accessor %sql-queue-in-transaction-p
                   :documentation "T while this object's outermost
                    transaction is open on CONNECTION (lock held).")
   (dirty :initform '() :accessor %sql-queue-dirty
          :documentation "Ids of subscribed jobs that got events since the
           last delivery, oldest first. A hint only: delivery reads the
           committed journal past each subscription's LAST-SEQ."))
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

;;; Implementation
;;;
;;; Mutation: targeted SQL statements on task_job inside one transaction
;;; per operation; every state change goes through %TRANSITION (one
;;; UPDATE + one JOB-TRANSITION event, checked against
;;; JOB-TRANSITION-ALLOWED-P). Admission is the claim SELECT's predicate.
;;;
;;; Delivery: the committed journal is the source. APPEND-JOB-EVENT only
;;; marks the job dirty; after the outermost COMMIT, still under the lock,
;;; %FLUSH pumps every subscription of the dirty jobs from the journal
;;; (SEQ > LAST-SEQ, in order). A rolled-back event was never committed,
;;; so it can never be delivered.
;;;
;;; Invariant (lock free, no open transaction): every subscription has
;;; been handed exactly its job's committed events with
;;; AFTER-SEQ < SEQ <= LAST-SEQ, in SEQ order, each once; every subscribed
;;; job with committed events past a subscription's LAST-SEQ is in DIRTY
;;; (only after a callback error escaped a delivery); usage is COUNT(*).

;;; SQL helpers

(defun %sql (queue sql &optional params)
  "Run SQL on QUEUE's connection to completion. → the rows (plists)."
  (sql-protocol:fetch-all
   (sql-protocol:execute (sql-job-queue-connection queue) sql params)))

(defun %sql-row (queue sql &optional params)
  "The first row of SQL (which should say LIMIT 1), or NIL."
  (first (%sql queue sql params)))

(defun %now (now)
  (or now (get-universal-time)))

(defun %read-restart-value (prompt)
  "Interactive reader for a USE-VALUE restart."
  (format *query-io* "~a: " prompt)
  (force-output *query-io*)
  (list (read *query-io*)))

;;; Transactions

(defun %rollback (queue)
  ;; Runs while unwinding: a failed ROLLBACK must not replace the
  ;; condition that is unwinding the operation.
  (handler-case (sql-protocol:execute (sql-job-queue-connection queue)
                                      "ROLLBACK")
    (sql-protocol:sql-error () nil)))

(defun %call-in-transaction (queue thunk)
  "Call THUNK with QUEUE's lock held inside one database transaction.
   The outermost call issues BEGIN IMMEDIATE, COMMIT on normal return
   (then delivers to subscriptions, lock held, no transaction open) and
   ROLLBACK on any non-local exit. Nested calls run THUNK in the open
   transaction. (sql-protocol 0.1.0 WITH-TRANSACTION has no IMMEDIATE
   mode, hence the plain statements.)"
  (bt:with-recursive-lock-held ((%sql-queue-lock queue))
    (if (%sql-queue-in-transaction-p queue)
        (funcall thunk)
        (let ((connection (sql-job-queue-connection queue))
              (committed nil))
          ;; If BEGIN signals (busy), nothing is open and nothing is owed.
          (sql-protocol:execute connection "BEGIN IMMEDIATE")
          (multiple-value-prog1
              (unwind-protect
                   (progn
                     (setf (%sql-queue-in-transaction-p queue) t)
                     (multiple-value-prog1 (funcall thunk)
                       (sql-protocol:execute connection "COMMIT")
                       (setf committed t)))
                (setf (%sql-queue-in-transaction-p queue) nil)
                (unless committed
                  (%rollback queue)))
            ;; Normal return only: committed.
            (%flush queue))))))

(defmacro %with-queue-transaction ((queue) &body body)
  `(%call-in-transaction ,queue (lambda () ,@body)))

(defmacro %with-queue-lock ((queue) &body body)
  "Pure reads: the lock, no transaction."
  `(bt:with-recursive-lock-held ((%sql-queue-lock ,queue)) ,@body))

;;; Row ↔ snapshot

(defun %state-keyword (text)
  (and text (intern (string-upcase text) :keyword)))

(defun %state-text (state)
  (string-downcase (symbol-name state)))

(defun %class-keyword (text)
  (and text (intern text :keyword)))

(defun %class-text (name)
  (and name (symbol-name name)))

(defun %column-text (value)
  "ENCODE-VALUE text of VALUE after CANONICALIZE-VALUE. May signal
   TASK-SERIALIZATION-ERROR (USE-VALUE)."
  (encode-value (task-protocol:canonicalize-value value)))

(defun %row->job (row)
  "A fresh TASK-PROTOCOL:JOB snapshot of a task_job ROW."
  (make-instance 'task-protocol:job
                 :id (getf row :id)
                 :kind (decode-value (getf row :kind))
                 :payload (decode-value (getf row :payload))
                 :priority (getf row :priority)
                 :resource-class (%class-keyword (getf row :resource_class))
                 :state (%state-keyword (getf row :state))
                 :submitted-at (getf row :submitted_at)
                 :started-at (getf row :started_at)
                 :finished-at (getf row :finished_at)
                 :updated-at (getf row :updated_at)
                 :result (decode-value (getf row :result))
                 :failure (decode-value (getf row :failure))
                 :worker-id (getf row :worker_id)
                 :lease-until (getf row :lease_until)
                 :attempts (getf row :attempts)
                 :cancel-requested (eql 1 (getf row :cancel_requested))))

(defun %job-row (queue id)
  (%sql-row queue "SELECT * FROM task_job WHERE id = ? LIMIT 1" (list id)))

(defun %job-snapshot (queue id)
  (%row->job (%job-row queue id)))

(defmacro %with-job ((row queue job-id &key (transaction t)) &body body)
  "Run BODY with ROW bound to JOB-ID's task_job row, in one transaction
   (TRANSACTION T) or under the lock only (NIL). An unknown id signals
   JOB-NOT-FOUND; the value given to its USE-VALUE restart is returned
   from the whole form, unwinding (rolling back) the transaction."
  (let ((q (gensym "QUEUE")) (id (gensym "ID")) (blk (gensym "WITH-JOB")))
    `(let ((,q ,queue) (,id ,job-id))
       (block ,blk
         (,(if transaction '%with-queue-transaction '%with-queue-lock) (,q)
          (let ((,row (or (%job-row ,q ,id)
                          (restart-case (error 'task-protocol:job-not-found
                                               :job-id ,id)
                            (use-value (value)
                              :report "Return a value from the operation instead."
                              :interactive (lambda ()
                                             (%read-restart-value "Value"))
                              (return-from ,blk value))))))
            ,@body))))))

(defun %worker-refused-p (row worker-id operation)
  "NIL when WORKER-ID holds the :running ROW. Otherwise signal
   JOB-STATE-ERROR (not :running) or JOB-LEASE-LOST (another holder) with
   a CONTINUE restart; → T when CONTINUE was taken."
  (let ((state (%state-keyword (getf row :state)))
        (owner (getf row :worker_id)))
    (restart-case
        (progn
          (cond ((not (eq state :running))
                 (error 'task-protocol:job-state-error
                        :job-id (getf row :id) :state state
                        :operation operation))
                ((not (equal owner worker-id))
                 (error 'task-protocol:job-lease-lost
                        :job-id (getf row :id) :state :running
                        :operation operation :worker-id worker-id
                        :owner owner)))
          nil)
      (continue ()
        :report "Leave the job unchanged."
        t))))

(defun %cancel-why (row)
  (or (decode-value (getf row :cancel_reason)) :canceled))

;;; The one state writer

(defun %transition (queue row to reason now
                    &key (worker-id (getf row :worker_id)) set)
  "Move ROW's job to state TO (call inside the operation's transaction):
   clears holder and lease when leaving :running, applies SET (an alist
   of column name → SQL value), stamps updated_at and, for a terminal
   state, finished_at, then journals one JOB-TRANSITION through
   APPEND-JOB-EVENT. Usage needs no bookkeeping: it is a COUNT(*)."
  (let* ((id (getf row :id))
         (from (%state-keyword (getf row :state)))
         (columns (append (when (eq from :running)
                            (list (cons "worker_id" nil)
                                  (cons "lease_until" nil)))
                          set
                          (list (cons "state" (%state-text to))
                                (cons "updated_at" now))
                          (when (task-protocol:job-terminal-state-p to)
                            (list (cons "finished_at" now)))))
         ;; Later entries win (SET overrides the clearing).
         (columns (remove-duplicates columns :key #'car :test #'string=)))
    (assert (task-protocol:job-transition-allowed-p from to) ()
            "job ~s: transition ~s → ~s is not allowed" id from to)
    ;; Column names are literals of this file; every value is a parameter.
    (%sql queue (format nil "UPDATE task_job SET ~{~a = ?~^, ~} WHERE id = ?"
                        (mapcar #'car columns))
          (append (mapcar #'cdr columns) (list id)))
    (task-protocol:append-job-event
     queue id (make-instance 'task-protocol:job-transition
                             :from-state from :to-state to
                             :worker-id worker-id :reason reason
                             :timestamp now))
    id))

;;; Resource classes

(defun %class-row (queue name)
  (and name (symbolp name)
       (%sql-row queue "SELECT name, job_limit FROM task_resource_class
 WHERE name = ? LIMIT 1"
                 (list (symbol-name name)))))

(defun %row->class (row)
  (make-instance 'task-protocol:resource-class
                 :name (%class-keyword (getf row :name))
                 :limit (getf row :job_limit)))

(defmethod task-protocol:define-resource-class ((queue sql-job-queue) name
                                                &key limit)
  (check-type name keyword)
  (check-type limit (integer 0))
  (%with-queue-transaction (queue)
    (if (%class-row queue name)
        ;; Same position; preempts nothing, only changes admission.
        (%sql queue "UPDATE task_resource_class SET job_limit = ? WHERE name = ?"
              (list limit (symbol-name name)))
        (%sql queue "INSERT INTO task_resource_class (name, job_limit, position)
 SELECT ?, ?, COALESCE(MAX(position), 0) + 1 FROM task_resource_class"
              (list (symbol-name name) limit)))
    (make-instance 'task-protocol:resource-class :name name :limit limit)))

(defmethod task-protocol:find-resource-class ((queue sql-job-queue) name)
  (%with-queue-lock (queue)
    (let ((row (%class-row queue name)))
      (and row (%row->class row)))))

(defmethod task-protocol:list-resource-classes ((queue sql-job-queue))
  (%with-queue-lock (queue)
    (mapcar #'%row->class
            (%sql queue "SELECT name, job_limit FROM task_resource_class
 ORDER BY position"))))

(defmethod task-protocol:resource-class-usage ((queue sql-job-queue) name)
  (if (and name (symbolp name))
      (%with-queue-lock (queue)
        (getf (%sql-row queue "SELECT COUNT(*) AS n FROM task_job
 WHERE resource_class = ? AND state = 'running'"
                        (list (symbol-name name)))
              :n 0))
      0))

;;; Client operations

(defun %journal-has-p (queue id)
  (let ((journal (task-protocol:job-queue-journal queue)))
    (if (sql-task-journal-p journal)
        (and (%sql-row queue (format nil "SELECT seq FROM ~a WHERE task_id = ? LIMIT 1"
                                     (%table-name (sql-journal-table journal)))
                       (list id))
             t)
        (and (task-protocol:journal-events journal id) t))))

(defun %fresh-job-id (queue n)
  "\"job-N\" from N upward, skipping ids in task_job or the journal (so
   the new job's seqs start at 1). Stateless: a reopened queue continues."
  (loop for k from n
        for id = (format nil "job-~d" k)
        unless (or (%job-row queue id) (%journal-has-p queue id))
          return id))

(defmethod task-protocol:submit-job ((queue sql-job-queue) kind
                                     &key payload (priority 0) resource-class now)
  (let ((now (%now now)))
    ;; Validate everything first: a failure leaves no id, row or event.
    (check-type priority integer)
    (let ((kind-text (%column-text kind))
          (payload-text (%column-text payload)))
      (loop while (and resource-class
                       (null (task-protocol:find-resource-class queue resource-class)))
            do (restart-case (error 'task-protocol:unknown-resource-class
                                    :name resource-class)
                 (use-value (name)
                   :report "Use another resource class (or NIL); it is checked again."
                   :interactive (lambda () (%read-restart-value "Class name"))
                   (setf resource-class name))))
      ;; Classes are never removed, so the name checked above stays valid.
      (%with-queue-transaction (queue)
        (let* ((seq (1+ (or (getf (%sql-row queue "SELECT MAX(submit_seq) AS m
 FROM task_job")
                                  :m)
                            0)))
               (id (%fresh-job-id queue seq)))
          (%sql queue "INSERT INTO task_job (id, submit_seq, kind, payload,
 priority, resource_class, state, submitted_at, updated_at, attempts,
 cancel_requested) VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, 0, 0)"
                (list id seq kind-text payload-text priority
                      (%class-text resource-class) now now))
          ;; The row exists (APPEND-JOB-EVENT checks it); the state machine
          ;; sees the move from no state.
          (%transition queue (list :id id :state nil) :queued :submitted now)
          id)))))

(defmethod task-protocol:job-status ((queue sql-job-queue) job-id)
  (%with-job (row queue job-id :transaction nil)
    (%row->job row)))

(defun %encodable-texts (values)
  "Column texts of VALUES; a value with no column text (a hash table)
   equals no stored text and is dropped."
  (loop for v in values
        for text = (handler-case (%column-text v)
                     (task-protocol:task-serialization-error () nil))
        when text collect text))

(defmethod task-protocol:list-jobs ((queue sql-job-queue)
                                    &key state kind resource-class)
  (let ((clauses '()) (params '()))
    (flet ((add (clause &rest values)
             (push clause clauses)
             (setf params (append params values))))
      (when state
        (let ((states (if (listp state) state (list state))))
          (apply #'add
                 (format nil "state IN (~{~a~^, ~})"
                         (mapcar (constantly "?") states))
                 (mapcar #'%state-text states))))
      (when kind
        (let ((texts (%encodable-texts (list kind))))
          (if texts
              (add "kind = ?" (first texts))
              (return-from task-protocol:list-jobs nil))))
      (when resource-class
        (add "resource_class = ?" (%class-text resource-class))))
    (%with-queue-lock (queue)
      (mapcar #'%row->job
              (%sql queue (format nil "SELECT * FROM task_job~@[ WHERE ~{~a~^ AND ~}~]
 ORDER BY submit_seq"
                                  (reverse clauses))
                    params)))))

(defmethod task-protocol:cancel-job ((queue sql-job-queue) job-id
                                     &key reason force now)
  (let ((now (%now now))
        (why (or reason :canceled)))
    (%with-job (row queue job-id)
      (ecase (%state-keyword (getf row :state))
        (:queued
         (%transition queue row :canceled :canceled now
                      :set (list (cons "failure" (%column-text why)))))
        (:running
         (cond (force
                (%transition queue row :canceled :forced now
                             :set (list (cons "failure"
                                              (%column-text
                                               (or reason
                                                   (decode-value
                                                    (getf row :cancel_reason))
                                                   :canceled))))))
               ((not (eql 1 (getf row :cancel_requested)))
                ;; A request changes no state and journals nothing.
                (%sql queue "UPDATE task_job SET cancel_requested = 1,
 cancel_reason = ?, updated_at = ? WHERE id = ?"
                      (list (%column-text why) now job-id)))))
        (:canceled nil)
        ((:completed :failed)
         (restart-case (error 'task-protocol:job-state-error
                              :job-id job-id
                              :state (%state-keyword (getf row :state))
                              :operation :cancel)
           (continue ()
             :report "Leave the job unchanged."
             nil))))
      (%job-snapshot queue job-id))))

(defmethod task-protocol:cancel-requested-p ((queue sql-job-queue) job-id)
  (%with-job (row queue job-id :transaction nil)
    (or (eql 1 (getf row :cancel_requested))
        (eq (%state-keyword (getf row :state)) :canceled))))

;;; Worker operations

(defun %claim-sql (kind-count)
  (format nil "SELECT j.* FROM task_job j
 LEFT JOIN task_resource_class rc ON rc.name = j.resource_class
 WHERE j.state = 'queued'~@[
   AND j.kind IN (~{~a~^, ~})~]
   AND (j.resource_class IS NULL OR rc.name IS NULL
        OR (SELECT COUNT(*) FROM task_job r
             WHERE r.resource_class = j.resource_class
               AND r.state = 'running') < rc.job_limit)
 ORDER BY j.priority DESC, j.submit_seq ASC
 LIMIT 1"
          (and (plusp kind-count)
               (make-list kind-count :initial-element "?"))))

(defmethod task-protocol:claim-job ((queue sql-job-queue) worker-id
                                    &key kinds now lease-seconds)
  (let* ((now (%now now))
         (lease-seconds (or lease-seconds 30))
         (texts (%encodable-texts kinds)))
    ;; One transaction: expire, scan, count usage, admit, update, journal.
    (%with-queue-transaction (queue)
      (task-protocol:expire-job-leases queue :now now)
      (unless (and kinds (null texts))
        (let ((row (%sql-row queue (%claim-sql (length texts)) texts)))
          (when row
            (%transition queue row :running :claimed now
                         :worker-id worker-id
                         :set (list (cons "worker_id" worker-id)
                                    (cons "lease_until" (+ now lease-seconds))
                                    (cons "started_at" now)
                                    (cons "attempts" (1+ (getf row :attempts)))))
            (%job-snapshot queue (getf row :id))))))))

(defmethod task-protocol:heartbeat-job ((queue sql-job-queue) job-id worker-id
                                        &key now lease-seconds)
  (let ((now (%now now))
        (lease-seconds (or lease-seconds 30)))
    (%with-job (row queue job-id)
      (unless (%worker-refused-p row worker-id :heartbeat)
        (%sql queue "UPDATE task_job SET lease_until = ?, updated_at = ?
 WHERE id = ?"
              (list (+ now lease-seconds) now job-id)))
      (%job-snapshot queue job-id))))

(defmethod task-protocol:complete-job ((queue sql-job-queue) job-id worker-id
                                       &key result now)
  (let ((now (%now now)))
    (%with-job (row queue job-id)
      (unless (%worker-refused-p row worker-id :complete)
        ;; May signal; nothing is written yet, so the job stays :running.
        (let ((text (%column-text result)))
          (%transition queue row :completed :completed now
                       :worker-id worker-id
                       :set (list (cons "result" text)))))
      (%job-snapshot queue job-id))))

(defmethod task-protocol:fail-job ((queue sql-job-queue) job-id worker-id
                                   &key error now)
  (let ((now (%now now)))
    (%with-job (row queue job-id)
      (unless (%worker-refused-p row worker-id :fail)
        (let ((text (%column-text (if (typep error 'condition)
                                      (princ-to-string error)
                                      error))))
          (%transition queue row :failed :failed now
                       :worker-id worker-id
                       :set (list (cons "failure" text)))))
      (%job-snapshot queue job-id))))

(defmethod task-protocol:acknowledge-cancel ((queue sql-job-queue) job-id
                                             worker-id &key now)
  (let ((now (%now now)))
    (%with-job (row queue job-id)
      (unless (%worker-refused-p row worker-id :acknowledge-cancel)
        (%transition queue row :canceled :canceled now
                     :worker-id worker-id
                     :set (list (cons "failure"
                                      (%column-text (%cancel-why row))))))
      (%job-snapshot queue job-id))))

(defmethod task-protocol:expire-job-leases ((queue sql-job-queue) &key now)
  (let ((now (%now now)))
    (%with-queue-transaction (queue)
      (loop for row in (%sql queue "SELECT * FROM task_job
 WHERE state = 'running' AND lease_until IS NOT NULL AND lease_until < ?
 ORDER BY submit_seq"
                             (list now))
            collect (progn
                      (if (eql 1 (getf row :cancel_requested))
                          (%transition queue row :canceled :lease-expired now
                                       :set (list (cons "failure"
                                                        (%column-text
                                                         (%cancel-why row)))))
                          (%transition queue row :queued :lease-expired now))
                      (%job-snapshot queue (getf row :id)))))))

;;; Events and subscriptions

(defun %events-after (queue job-id after-seq)
  "JOB-ID's journaled events with SEQ > AFTER-SEQ, in SEQ order."
  (let ((journal (task-protocol:job-queue-journal queue)))
    (if (sql-task-journal-p journal)
        (mapcar (lambda (row) (decode-event (%row-get row :payload)))
                (%fetch-all journal
                            (format nil "SELECT payload FROM ~a
 WHERE task_id = ? AND seq > ? ORDER BY seq"
                                    (%table-name (sql-journal-table journal)))
                            (list job-id after-seq)))
        (remove-if-not (lambda (e) (> (task-protocol:event-seq e) after-seq))
                       (task-protocol:journal-events journal job-id)))))

(defun %deliver (sub event)
  "Hand EVENT to SUB if it is active and EVENT is past its LAST-SEQ.
   LAST-SEQ moves before the call, so a skipped delivery is not repeated."
  (when (and (task-protocol:job-subscription-active-p sub)
             (> (task-protocol:event-seq event)
                (task-protocol:job-subscription-last-seq sub)))
    (setf (task-protocol:job-subscription-last-seq sub)
          (task-protocol:event-seq event))
    (restart-case (funcall (task-protocol:job-subscription-callback sub) event)
      (continue ()
        :report "Skip this delivery; the event stays journaled."
        nil))))

(defun %pump (queue sub)
  "Deliver to SUB, in SEQ order, every committed event of its job past
   its LAST-SEQ."
  (when (task-protocol:job-subscription-active-p sub)
    (dolist (e (%events-after queue (task-protocol:job-subscription-job-id sub)
                              (task-protocol:job-subscription-last-seq sub)))
      (%deliver sub e))))

(defun %touch (queue job-id)
  "Mark JOB-ID for delivery after the outermost commit."
  (when (and (gethash job-id (%sql-queue-subscriptions queue))
             (not (member job-id (%sql-queue-dirty queue) :test #'equal)))
    (setf (%sql-queue-dirty queue)
          (append (%sql-queue-dirty queue) (list job-id)))))

(defun %flush (queue)
  "Pump the subscriptions of every dirty job (lock held, no transaction).
   Ids are taken one at a time: if a callback error escapes, the rest stay
   dirty for the next commit."
  (loop while (%sql-queue-dirty queue)
        do (let ((id (pop (%sql-queue-dirty queue))))
             ;; A copy: a callback may unsubscribe.
             (dolist (sub (copy-list (gethash id (%sql-queue-subscriptions queue))))
               (%pump queue sub)))))

(defmethod task-protocol:append-job-event ((queue sql-job-queue) job-id event)
  (%with-job (row queue job-id)
    (declare (ignore row))
    (setf (task-protocol:event-task-id event) job-id)
    ;; Inside the transaction: the journal's MAX(seq)+1 is race-free.
    (task-protocol:append-event (task-protocol:job-queue-journal queue) event)
    (%touch queue job-id)
    event))

(defmethod task-protocol:report-job-progress ((queue sql-job-queue) job-id
                                              worker-id
                                              &key phase fraction message now)
  (let ((now (%now now)))
    (%with-job (row queue job-id)
      ;; The state check comes first: CONTINUE there journals nothing.
      (unless (%worker-refused-p row worker-id :progress)
        (loop until (or (null fraction)
                        (and (realp fraction) (<= 0 fraction 1)))
              do (restart-case (error 'task-protocol:job-progress-error
                                      :job-id job-id :fraction fraction)
                   (use-value (value)
                     :report "Use another fraction; it is checked again."
                     :interactive (lambda () (%read-restart-value "Fraction"))
                     (setf fraction value))))
        (task-protocol:append-job-event
         queue job-id
         (make-instance 'task-protocol:job-progress
                        :phase phase :fraction fraction
                        :message message :worker-id worker-id
                        :timestamp now))))))

(defmethod task-protocol:job-events ((queue sql-job-queue) job-id
                                     &key after-seq)
  (%with-job (row queue job-id :transaction nil)
    (declare (ignore row))
    (%events-after queue job-id (or after-seq 0))))

(defmethod task-protocol:subscribe-job-events ((queue sql-job-queue) job-id
                                               callback &key after-seq)
  (%with-job (row queue job-id :transaction nil)
    (declare (ignore row))
    (let ((sub (make-instance 'task-protocol:job-subscription
                              :job-id job-id :callback callback
                              :last-seq (or after-seq 0))))
      ;; Register, then replay, under the lock every appender takes.
      (setf (gethash job-id (%sql-queue-subscriptions queue))
            (append (gethash job-id (%sql-queue-subscriptions queue))
                    (list sub)))
      (if (%sql-queue-in-transaction-p queue)
          ;; Called inside an open transaction (from a handler): replay
          ;; after its commit, never uncommitted events.
          (%touch queue job-id)
          (%pump queue sub))
      sub)))

(defmethod task-protocol:unsubscribe-job-events ((queue sql-job-queue)
                                                 subscription)
  (%with-queue-lock (queue)
    (setf (task-protocol:job-subscription-active-p subscription) nil)
    (let ((id (task-protocol:job-subscription-job-id subscription)))
      (setf (gethash id (%sql-queue-subscriptions queue))
            (remove subscription (gethash id (%sql-queue-subscriptions queue)))))
    subscription))
