(in-package #:task-backend-sql/tests)

;;; Schema/codec tests stay connection-free. Live SQLite kill-and-resume is
;;; in resume-test.lisp (sql-backend-sqlite3 is a test dependency).

(deftest journal-ddl-mentions-columns
  (ok (search "task_id" task-backend-sql:+journal-ddl+))
  (ok (search "seq" task-backend-sql:+journal-ddl+))
  (ok (search "type" task-backend-sql:+journal-ddl+))
  (ok (search "payload" task-backend-sql:+journal-ddl+))
  (ok (search "CREATE TABLE" task-backend-sql:+journal-ddl+)))

(deftest lease-ddl-mentions-columns
  (ok (search "task_id" task-backend-sql:+lease-ddl+))
  (ok (search "worker_id" task-backend-sql:+lease-ddl+))
  (ok (search "lease_until" task-backend-sql:+lease-ddl+))
  (ok (search "heartbeat_at" task-backend-sql:+lease-ddl+)))

(deftest revision-metadata
  (let ((p (task-backend-sql:journal-revision-plist)))
    (ok (equal "0001" (getf p :id)))
    (ok (equal "task-journal" (getf p :name)))
    (ok (null (getf p :down-revision)))
    (ok (find task-backend-sql:+journal-ddl+ (getf p :upgrade-sql)
              :test #'equal))))

(deftest encode-decode-event-roundtrip
  (let* ((ev (make-instance 'task-protocol:step-completed
                            :task-id "t1"
                            :seq 3
                            :name "step"
                            :result (list :n 2 :ok t)
                            :idempotency-key "k"))
         (text (task-backend-sql:encode-event ev))
         (copy (task-backend-sql:decode-event text)))
    (ok (stringp text))
    (ok (typep copy 'task-protocol:step-completed))
    (ok (equal "t1" (task-protocol:event-task-id copy)))
    (ok (equal "step" (task-protocol:step-name copy)))
    (ok (equal (list :n 2 :ok t) (task-protocol:step-result copy)))
    (ok (equal "k" (task-protocol:step-idempotency-key copy)))))

(deftest lease-sql-is-sqlite-friendly
  (let ((select (task-backend-sql:claimable-lease-sql "task_lease"))
        (update (task-backend-sql:claim-lease-sql "task_lease")))
    (ok (search "lease_until IS NULL OR lease_until <" select))
    (ok (search "LIMIT 1" select))
    (ng (search "SKIP LOCKED" select))
    (ok (search "worker_id" update))
    (ok (search "heartbeat" (task-backend-sql:heartbeat-sql "task_lease")))
    (ok (search "worker_id = NULL" (task-backend-sql:release-lease-sql
                                    "task_lease")))))

(deftest lease-claimable-predicate
  (ok (task-backend-sql:lease-claimable-p nil 100))
  (ok (task-backend-sql:lease-claimable-p 50 100))
  (ng (task-backend-sql:lease-claimable-p 150 100)))

(deftest invalid-table-name
  (ok (signals (task-backend-sql:make-sql-task-journal
                :connection nil
                :table "journal;drop"
                :ensure-schema nil)
               'task-protocol:task-error)))
