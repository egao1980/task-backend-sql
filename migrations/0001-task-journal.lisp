;;;; sql-migrate revision 0001 — task journal + worker leases
;;;;
;;;; sql-migrate 0.1.0 stores revisions in a SCRIPT-DIRECTORY (in-memory);
;;;; there is no on-disk runner format yet. Register this revision with:
;;;;
;;;;   (ql:quickload '(:sql-migrate :sql-orm :task-backend-sql))
;;;;   (let ((dir (sql-migrate:make-script-directory)))
;;;;     (sql-migrate:register-revision
;;;;      dir (task-backend-sql:make-journal-revision)))
;;;;
;;;; Wave-1 applies the same DDL via ENSURE-JOURNAL-SCHEMA / +journal-ddl+
;;;; (no hard dependency on sql-migrate or sql-orm).

(in-package #:task-backend-sql)

(defparameter +revision-0001+
  (journal-revision-plist))
