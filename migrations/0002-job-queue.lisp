;;;; sql-migrate revision 0002 — job queue (task-protocol 0.3.0 queue)
;;;;
;;;; Down revision: 0001 (task journal + worker leases). Upgrade creates
;;;; task_job, task_resource_class and their indexes (+job-ddl+,
;;;; +resource-class-ddl+, +job-index-ddl+); downgrade drops them and leaves
;;;; the journal. Job events stay in 0001's task_journal.
;;;;
;;;; As with 0001, sql-migrate 0.1.0 keeps revisions in an in-memory
;;;; SCRIPT-DIRECTORY; MAKE-SQL-JOB-QUEUE / ENSURE-QUEUE-SCHEMA apply the same
;;;; DDL directly (no hard dependency on sql-migrate or sql-orm).

(in-package #:task-backend-sql)

(defparameter +revision-0002+
  (queue-revision-plist))
