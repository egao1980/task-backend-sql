(in-package #:task-backend-sql)

;;; 0.2.0 (given): job queue schema, revision 0002, and the column codec.
;;;
;;; Tables
;;; ------
;;; task_job             one row per job (the job record of task-protocol
;;;                      0.3.0, src/jobs.lisp). SUBMIT_SEQ is the FIFO key:
;;;                      assigned once at submit (MAX + 1 inside the submit
;;;                      transaction), never changed, so a job re-queued by
;;;                      lease expiry keeps its place. SUBMITTED_AT is a
;;;                      timestamp only (several jobs may share one NOW).
;;; task_resource_class  NAME (keyword's SYMBOL-NAME), JOB_LIMIT, POSITION
;;;                      (definition order). Kept in the database so every
;;;                      connection checks the same limit inside its claim
;;;                      transaction. Usage is not stored: it is
;;;                      COUNT(*) of running jobs of the class, so it cannot
;;;                      drift from the job rows.
;;; task_journal         (revision 0001) carries every job event: the
;;;                      JOB-TRANSITIONs and JOB-PROGRESSes, task_id = job id.
;;;
;;; Column encodings
;;; ----------------
;;; STATE is the state keyword's name in lower case ("queued", …).
;;; RESOURCE_CLASS is the keyword's SYMBOL-NAME ("HEAP-4G") or NULL.
;;; KIND, PAYLOAD, RESULT, FAILURE and CANCEL_REASON hold ENCODE-VALUE text
;;; (never NULL for KIND, so a KINDS filter compares encoded text; two kinds
;;; are EQUAL exactly when their encodings are STRING=, for the values
;;; CANONICALIZE-VALUE admits other than hash tables).
;;; CANCEL_REQUESTED is 0 or 1. Times are universal times (BIGINT: they do
;;; not fit a 32-bit INTEGER on Postgres).

(defparameter +queue-revision-id+ "0002")
(defparameter +queue-revision-name+ "job-queue")

(defparameter +job-ddl+
  "CREATE TABLE IF NOT EXISTS task_job (
  id TEXT NOT NULL PRIMARY KEY,
  submit_seq BIGINT NOT NULL UNIQUE,
  kind TEXT NOT NULL,
  payload TEXT,
  priority BIGINT NOT NULL DEFAULT 0,
  resource_class TEXT,
  state TEXT NOT NULL,
  submitted_at BIGINT,
  started_at BIGINT,
  finished_at BIGINT,
  updated_at BIGINT,
  result TEXT,
  failure TEXT,
  worker_id TEXT,
  lease_until BIGINT,
  attempts BIGINT NOT NULL DEFAULT 0,
  cancel_requested INTEGER NOT NULL DEFAULT 0,
  cancel_reason TEXT
)")

(defparameter +resource-class-ddl+
  "CREATE TABLE IF NOT EXISTS task_resource_class (
  name TEXT NOT NULL PRIMARY KEY,
  job_limit BIGINT NOT NULL,
  position BIGINT NOT NULL
)")

(defparameter +job-index-ddl+
  (list
   ;; Claim scan: queued jobs by priority, then FIFO.
   "CREATE INDEX IF NOT EXISTS task_job_claim_idx
  ON task_job (state, priority, submit_seq)"
   ;; Admission: running jobs of one class.
   "CREATE INDEX IF NOT EXISTS task_job_class_idx
  ON task_job (resource_class, state)"
   ;; Expiry: running jobs by lease end.
   "CREATE INDEX IF NOT EXISTS task_job_lease_idx
  ON task_job (state, lease_until)")
  "Indexes of revision 0002, in creation order.")

(defun queue-revision-plist ()
  "sql-migrate-shaped metadata of revision 0002 (job queue), on top of 0001.
   Applying 0001's then 0002's :UPGRADE-SQL to an empty database gives the
   full 0.2.0 schema; 0002's :DOWNGRADE-SQL drops only the queue tables."
  (list :id +queue-revision-id+
        :name +queue-revision-name+
        :down-revision +journal-revision-id+
        :upgrade-sql (list* +job-ddl+ +resource-class-ddl+ +job-index-ddl+)
        :downgrade-sql (list "DROP INDEX IF EXISTS task_job_lease_idx"
                             "DROP INDEX IF EXISTS task_job_class_idx"
                             "DROP INDEX IF EXISTS task_job_claim_idx"
                             "DROP TABLE IF EXISTS task_resource_class"
                             "DROP TABLE IF EXISTS task_job")))

(defgeneric ensure-queue-schema (target)
  (:documentation
   "Apply revisions 0001 and 0002 idempotently (CREATE … IF NOT EXISTS) on
    TARGET, a SQL-PROTOCOL:SQL-CONNECTION or a SQL-JOB-QUEUE (its
    connection). → TARGET."))

(defmethod ensure-queue-schema ((connection sql-protocol:sql-connection))
  (dolist (sql (append (getf (journal-revision-plist) :upgrade-sql)
                       (getf (queue-revision-plist) :upgrade-sql)))
    (sql-protocol:execute connection sql))
  connection)

;;; Column codec

(defun encode-value (value)
  "Text for a KIND, PAYLOAD, RESULT, FAILURE or CANCEL_REASON column: VALUE
   (already through CANONICALIZE-VALUE) printed readably with standard
   syntax, symbols qualified relative to CL. NIL encodes as \"NIL\".
   A value with no readable printed form (a hash table, which
   CANONICALIZE-VALUE admits) signals TASK-SERIALIZATION-ERROR with a
   USE-VALUE restart whose value is encoded instead (checked again)."
  (loop
    (handler-case
        (return
          (with-standard-io-syntax
            (let ((*print-readably* t)
                  (*read-eval* nil)
                  (*print-circle* nil)
                  (*print-pretty* nil)
                  (*package* (find-package :cl)))
              (prin1-to-string value))))
      (print-not-readable ()
        (restart-case
            (error 'task-protocol:task-serialization-error
                   :value value
                   :message (format nil "no readable SQL column text for ~s"
                                    value))
          (use-value (replacement)
            :report "Encode a substitute value instead (checked again)."
            :interactive (lambda ()
                           (format *query-io* "Value: ")
                           (force-output *query-io*)
                           (list (read *query-io*)))
            (setf value replacement)))))))

(defun decode-value (text)
  "Inverse of ENCODE-VALUE. SQL NULL (NIL) decodes as NIL. Reads with
   *READ-EVAL* NIL: a #. form signals a READER-ERROR, never evaluates."
  (if (null text)
      nil
      (with-standard-io-syntax
        (let ((*read-eval* nil)
              (*package* (find-package :cl)))
          (values (read-from-string text))))))
