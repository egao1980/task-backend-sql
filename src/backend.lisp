(in-package #:task-backend-sql)

(defclass sql-task-journal ()
  ((connection :initarg :connection :accessor sql-journal-connection)
   (table :initarg :table :accessor sql-journal-table :initform "task_journal")
   (lease-table :initarg :lease-table :accessor sql-journal-lease-table
                :initform "task_lease")
   (redaction-policy :initarg :redaction-policy
                     :accessor task-protocol:journal-redaction-policy
                     :initform (task-protocol:make-redaction-policy))
   (retention-policy :initarg :retention-policy
                     :accessor task-protocol:journal-retention-policy
                     :initform nil)))

(defun sql-task-journal-p (x)
  (typep x 'sql-task-journal))

(defun encode-event (event)
  "Serialize EVENT via task-protocol 0.2.1 ENCODE-EVENT.

   When serdes/json is bound, persist :json-object (hash-table → JSON object).
   encode-payload of a keyword plist would become a JSON array, which
   DECODE-EVENT refuses to guess as a plist. Without JSON, prin1 the
   sexp-plist so tests stay connection-free."
  (if (%json-wire-p)
      (task-protocol:encode-payload
       (task-protocol:encode-event event :codec :json-object))
      (with-standard-io-syntax
        (let ((*print-readably* nil)
              (*print-circle* t)
              (*print-pretty* nil)
              (*package* (find-package :cl)))
          (prin1-to-string
           (task-protocol:encode-event event :codec :sexp-plist))))))

(defun %json-wire-p ()
  "T when ENCODE-PAYLOAD would emit JSON (serdes *serdes-format* = :json)."
  (let* ((pkg (find-package '#:serdes-protocol))
         (fmt (and pkg (find-symbol "*SERDES-FORMAT*" pkg))))
    (and fmt (boundp fmt)
         (let ((v (symbol-value fmt)))
           (or (eq v :json)
               (and (symbolp v)
                    (equal (symbol-name v) "JSON")))))))

(defun decode-event (payload)
  "Rehydrate a TASK-EVENT via task-protocol DECODE-EVENT.
   Does not replace EVENT-FROM-PLIST; vectors stay arrays."
  (let ((data (if (stringp payload)
                  (task-protocol:decode-payload payload)
                  payload)))
    (if (typep data 'task-protocol:task-event)
        data
        (task-protocol:decode-event data))))

(defun %exec (journal sql &optional params)
  (sql-protocol:execute (sql-journal-connection journal) sql params))

(defun %fetch (journal sql &optional params)
  (sql-protocol:fetch (%exec journal sql params)))

(defun %fetch-all (journal sql &optional params)
  (sql-protocol:fetch-all (%exec journal sql params)))

(defun %row-get (row key)
  (or (getf row key)
      (getf row (intern (string-upcase (string key)) :keyword))))

(defmethod ensure-journal-schema ((journal sql-task-journal))
  (let ((jtable (%table-name (sql-journal-table journal)))
        (ltable (%table-name (sql-journal-lease-table journal))))
    (%exec journal (%ddl-for-table +journal-ddl+ "task_journal" jtable))
    (%exec journal (%ddl-for-table +lease-ddl+ "task_lease" ltable)))
  journal)

(defmethod ensure-journal-schema ((connection sql-protocol:sql-connection))
  (sql-protocol:execute connection +journal-ddl+)
  (sql-protocol:execute connection +lease-ddl+)
  connection)

(defun make-sql-task-journal (&key connection table lease-table
                                redaction-policy retention-policy
                                (ensure-schema t))
  (let ((journal (make-instance 'sql-task-journal
                                :connection connection
                                :table (%table-name (or table "task_journal"))
                                :lease-table (%table-name (or lease-table
                                                              "task_lease"))
                                :redaction-policy (or redaction-policy
                                                      (task-protocol:make-redaction-policy))
                                :retention-policy retention-policy)))
    (when (and ensure-schema connection)
      (ensure-journal-schema journal))
    journal))

(defun %max-seq (journal task-id)
  (let ((row (%fetch journal
                     (format nil "SELECT MAX(seq) AS seq FROM ~a WHERE task_id = ?"
                             (%table-name (sql-journal-table journal)))
                     (list task-id))))
    (or (and row (%row-get row :seq)) 0)))

(defun %upsert-lease-status (journal task-id status)
  (let ((table (%table-name (sql-journal-lease-table journal))))
    (%exec journal
           (format nil "DELETE FROM ~a WHERE task_id = ?" table)
           (list task-id))
    (%exec journal
           (format nil "INSERT INTO ~a (task_id, worker_id, lease_until, heartbeat_at, status)
VALUES (?, NULL, NULL, NULL, ?)"
                   table)
           (list task-id (string-downcase (symbol-name status))))))

(defmethod task-protocol:append-event ((journal sql-task-journal) event)
  (let* ((id (or (task-protocol:event-task-id event)
                 (and task-protocol:*task*
                      (task-protocol:durable-task-id task-protocol:*task*))))
         (seq (or (task-protocol:event-seq event)
                  (1+ (%max-seq journal id))))
         (table (%table-name (sql-journal-table journal))))
    (unless id
      (error 'task-protocol:task-error :message "event has no task-id"))
    (setf (task-protocol:event-task-id event) id
          (task-protocol:event-seq event) seq)
    (%exec journal
           (format nil "INSERT INTO ~a (task_id, seq, type, payload) VALUES (?, ?, ?, ?)"
                   table)
           (list id
                 seq
                 (string-downcase
                  (symbol-name (task-protocol:event-type-keyword event)))
                 (encode-event event)))
    (when (typep event 'task-protocol:task-started)
      (%upsert-lease-status journal id :running))
    (when (typep event 'task-protocol:task-completed)
      (%upsert-lease-status journal id :completed))
    (when (typep event 'task-protocol:task-failed)
      (%upsert-lease-status journal id
                            (if (eq (task-protocol:event-reason event) :canceled)
                                :canceled
                                :failed)))
    event))

(defmethod task-protocol:journal-events ((journal sql-task-journal) task)
  (let* ((id (if (task-protocol:durable-task-p task)
                 (task-protocol:durable-task-id task)
                 task))
         (rows (%fetch-all
                journal
                (format nil "SELECT task_id, seq, type, payload FROM ~a
 WHERE task_id = ? ORDER BY seq"
                        (%table-name (sql-journal-table journal)))
                (list id))))
    (mapcar (lambda (row)
              (decode-event (%row-get row :payload)))
            rows)))

(defmethod task-protocol:journal-task-ids ((journal sql-task-journal))
  (mapcar (lambda (row) (%row-get row :task_id))
          (%fetch-all journal
                      (format nil "SELECT DISTINCT task_id FROM ~a ORDER BY task_id"
                              (%table-name (sql-journal-table journal))))))

(defmethod task-protocol:import-events ((journal sql-task-journal) events)
  (dolist (event events journal)
    (let ((e (if (typep event 'task-protocol:task-event)
                 (task-protocol:copy-event event)
                 (task-protocol:decode-event event))))
      (task-protocol:append-event journal e))))

(defmethod task-protocol:replay-journal ((journal sql-task-journal) task)
  (task-protocol:replay-journal
   (let ((mem (task-protocol:make-in-memory-journal
               :redaction-policy nil)))
     (task-protocol:import-events mem (task-protocol:journal-events journal task))
     mem)
   task)
  task)

(defmethod task-protocol:compact-journal ((journal sql-task-journal) task)
  (let* ((id (if (task-protocol:durable-task-p task)
                 (task-protocol:durable-task-id task)
                 task))
         (events (task-protocol:journal-events journal task))
         (snap (make-instance 'task-protocol:journal-snapshot
                              :task-id id
                              :status (task-protocol:durable-task-status task)
                              :result (task-protocol:durable-task-result task)
                              :steps (mapcan (lambda (e)
                                               (when (typep e 'task-protocol:step-completed)
                                                 (list (task-protocol:event-plist e))))
                                             events)
                              :event-count (length events)))
         (table (%table-name (sql-journal-table journal))))
    (task-protocol:redact-event
     (task-protocol:journal-redaction-policy journal) snap)
    (%exec journal (format nil "DELETE FROM ~a WHERE task_id = ?" table)
           (list id))
    (setf (task-protocol:event-seq snap) 1)
    (task-protocol:append-event journal snap)
    snap))

;;; Worker leases — SQLite-friendly (no SKIP LOCKED).
;;; TODO: Postgres method with SELECT … FOR UPDATE SKIP LOCKED for multi-worker.

(defun lease-claimable-p (lease-until now)
  (or (null lease-until) (< lease-until now)))

(defun claimable-lease-sql (lease-table)
  (format nil "SELECT task_id, worker_id, lease_until, heartbeat_at, status
 FROM ~a
 WHERE (lease_until IS NULL OR lease_until < ?)
   AND (status IS NULL OR status IN ('new', 'running', 'waiting'))
 ORDER BY task_id
 LIMIT 1"
          (%table-name lease-table)))

(defun claim-lease-sql (lease-table)
  (format nil "UPDATE ~a SET worker_id = ?, lease_until = ?, heartbeat_at = ?
 WHERE task_id = ?
   AND (lease_until IS NULL OR lease_until < ?)"
          (%table-name lease-table)))

(defun heartbeat-sql (lease-table)
  (format nil "UPDATE ~a SET lease_until = ?, heartbeat_at = ?
 WHERE task_id = ? AND worker_id = ?"
          (%table-name lease-table)))

(defun release-lease-sql (lease-table)
  (format nil "UPDATE ~a SET worker_id = NULL, lease_until = NULL
 WHERE task_id = ? AND worker_id = ?"
          (%table-name lease-table)))

(defun claim-task (journal worker-id &key (now (get-universal-time))
                                    (lease-seconds 30))
  "Claim one expired/unleased task for WORKER-ID. SQLite-friendly (no SKIP LOCKED)."
  (check-type worker-id string)
  (let* ((table (sql-journal-lease-table journal))
         (row (%fetch journal (claimable-lease-sql table) (list now))))
    (when row
      (let ((task-id (%row-get row :task_id))
            (until (+ now lease-seconds)))
        (%exec journal (claim-lease-sql table)
               (list worker-id until now task-id now))
        task-id))))

(defun heartbeat (journal task-id worker-id &key (now (get-universal-time))
                                           (lease-seconds 30))
  (check-type task-id string)
  (check-type worker-id string)
  (%exec journal (heartbeat-sql (sql-journal-lease-table journal))
         (list (+ now lease-seconds) now task-id worker-id))
  task-id)

(defun release-task (journal task-id worker-id)
  (check-type task-id string)
  (check-type worker-id string)
  (%exec journal (release-lease-sql (sql-journal-lease-table journal))
         (list task-id worker-id))
  task-id)
