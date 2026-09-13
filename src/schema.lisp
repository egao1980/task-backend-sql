(in-package #:task-backend-sql)

;;; Wave-1 applies DDL via ENSURE-JOURNAL-SCHEMA (sql-protocol:execute).
;;; sql-migrate file layout is still in-memory revisions (see sql-migrate
;;; README). REGISTER a revision with MAKE-JOURNAL-REVISION when sql-orm +
;;; sql-migrate are loaded. Live SQLite/Postgres tests are optional — see
;;; README.

(defparameter +journal-revision-id+ "0001")
(defparameter +journal-revision-name+ "task-journal")

(defparameter +journal-ddl+
  "CREATE TABLE IF NOT EXISTS task_journal (
  task_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (task_id, seq)
)")

(defparameter +lease-ddl+
  "CREATE TABLE IF NOT EXISTS task_lease (
  task_id TEXT NOT NULL PRIMARY KEY,
  worker_id TEXT,
  lease_until INTEGER,
  heartbeat_at INTEGER,
  status TEXT
)")

(defun journal-revision-plist ()
  "sql-migrate-shaped revision metadata (upgrade SQL is +journal-ddl+ / +lease-ddl+)."
  (list :id +journal-revision-id+
        :name +journal-revision-name+
        :down-revision nil
        :upgrade-sql (list +journal-ddl+ +lease-ddl+)
        :downgrade-sql (list "DROP TABLE IF EXISTS task_lease"
                             "DROP TABLE IF EXISTS task_journal")))

(defgeneric ensure-journal-schema (target)
  (:documentation "CREATE TABLE IF NOT EXISTS journal + lease tables."))

(defun %table-name (name)
  (let ((s (string name)))
    (unless (and (plusp (length s))
                 (alpha-char-p (char s 0))
                 (every (lambda (c)
                          (or (alphanumericp c) (char= c #\_)))
                        s))
      (error 'task-protocol:task-error
             :message (format nil "invalid table name ~s" name)))
    s))

(defun %ddl-for-table (template default-name table)
  (if (string= table default-name)
      template
      (let ((pos (search default-name template)))
        (if pos
            (concatenate 'string
                         (subseq template 0 pos)
                         table
                         (subseq template (+ pos (length default-name))))
            template))))

(defun make-journal-revision (&optional dir)
  "Return a sql-orm SCHEMA-MIGRATION when sql-orm is loaded; otherwise the
   revision plist. If DIR is a sql-migrate SCRIPT-DIRECTORY, register it."
  (let* ((orm (find-package '#:sql-orm))
         (migrate (find-package '#:sql-migrate))
         (meta (journal-revision-plist))
         (rev
          (if orm
              (let ((make (find-symbol "MAKE-INSTANCE" :cl))
                    (class (find-symbol "SCHEMA-MIGRATION" orm)))
                (declare (ignore make))
                (if class
                    (make-instance class
                                   :name +journal-revision-name+
                                   :revision +journal-revision-id+
                                   :down-revision nil
                                   :ops nil)
                    meta))
              meta)))
    (when (and dir migrate)
      (let ((reg (find-symbol "REGISTER-REVISION" migrate)))
        (when (and reg (fboundp reg))
          (funcall reg dir rev))))
    rev))
