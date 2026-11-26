(defpackage #:task-backend-sql
  (:use #:cl)
  (:export #:sql-task-journal
           #:sql-task-journal-p
           #:make-sql-task-journal
           #:sql-journal-connection
           #:sql-journal-table
           #:sql-journal-lease-table

           #:+journal-ddl+
           #:+lease-ddl+
           #:+journal-revision-id+
           #:+journal-revision-name+
           #:ensure-journal-schema
           #:make-journal-revision
           #:journal-revision-plist

           #:encode-event
           #:decode-event

           #:claim-task
           #:heartbeat
           #:release-task
           #:claimable-lease-sql
           #:claim-lease-sql
           #:heartbeat-sql
           #:release-lease-sql
           #:lease-claimable-p

           ;; 0.2.0: SQL job queue for the task-protocol 0.3.0 queue
           ;; generics (submit-job, claim-job, … are task-protocol's).
           #:sql-job-queue
           #:sql-job-queue-p
           #:make-sql-job-queue
           #:sql-job-queue-connection
           #:sql-job-queue-dialect
           #:sql-job-queue-busy-timeout

           #:+job-ddl+
           #:+resource-class-ddl+
           #:+job-index-ddl+
           #:+queue-revision-id+
           #:+queue-revision-name+
           #:queue-revision-plist
           #:ensure-queue-schema

           #:encode-value
           #:decode-value))

(in-package #:task-backend-sql)
