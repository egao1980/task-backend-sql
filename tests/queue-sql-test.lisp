(in-package #:task-backend-sql/tests)

;;; 0.2.0 contract: what only the SQL job queue has. Schema and revision
;;; 0002, the column codec, several connections over one SQLite file
;;; (atomic admission, reopening, polling, delivery after commit) and
;;; rollback of an unwound operation. Helpers come from
;;; conformance-support.lisp (%CONNECTION, %CATCH, …) and
;;; conformance-progress-test.lisp (%COLLECTOR).

(defun %temp-db-dir ()
  (ensure-directories-exist
   (merge-pathnames (format nil "task-queue-~d-~d/"
                            (get-universal-time) (random 1000000))
                    (uiop:temporary-directory))))

(defmacro %with-temp-db ((path) &body body)
  "Bind PATH to the native namestring of a fresh SQLite file; close every
   test connection and delete the file's directory afterwards."
  (let ((dir (gensym "DIR")))
    `(let* ((,dir (%temp-db-dir))
            (,path (uiop:native-namestring
                    (merge-pathnames "queue.sqlite" ,dir))))
       (unwind-protect (progn ,@body)
         (%close-test-connections)
         (ignore-errors
           (uiop:delete-directory-tree
            ,dir
            :validate (lambda (p) (search "task-queue-" (namestring p)))
            :if-does-not-exist :ignore))))))

(defun %disconnect (connection)
  "Close CONNECTION now (a simulated process exit); the after-test hook
   will not close it again."
  (bt:with-lock-held (*test-connections-lock*)
    (setf *test-connections* (remove connection *test-connections*)))
  (sql-protocol:disconnect connection))

(defun %file-queue (path &rest keys)
  "A SQL job queue on a new connection to the SQLite file PATH."
  (apply #'task-backend-sql:make-sql-job-queue
         :connection (%connection path) keys))

(defun %schema-names (connection)
  (mapcar (lambda (row) (getf row :name))
          (sql-protocol:fetch-all
           (sql-protocol:execute connection "SELECT name FROM sqlite_master"))))

(defun %class-pairs (queue)
  (mapcar (lambda (rc)
            (list (stack-task:resource-class-name rc)
                  (stack-task:resource-class-limit rc)))
          (stack-task:list-resource-classes queue)))

(defparameter +queue-schema-names+
  '("task_journal" "task_lease" "task_job" "task_resource_class"
    "task_job_claim_idx" "task_job_class_idx" "task_job_lease_idx"))

;;; Schema and revision 0002 (given)

(deftest queue-ddl-mentions-columns
  (dolist (col '("id" "submit_seq" "kind" "payload" "priority"
                 "resource_class" "state" "submitted_at" "started_at"
                 "finished_at" "updated_at" "result" "failure" "worker_id"
                 "lease_until" "attempts" "cancel_requested" "cancel_reason"))
    (ok (search col task-backend-sql:+job-ddl+) col))
  (ok (search "CREATE TABLE IF NOT EXISTS task_job" task-backend-sql:+job-ddl+))
  (ok (search "CREATE TABLE IF NOT EXISTS task_resource_class"
              task-backend-sql:+resource-class-ddl+))
  (ok (search "job_limit" task-backend-sql:+resource-class-ddl+))
  (ok (search "position" task-backend-sql:+resource-class-ddl+))
  (ok (= 3 (length task-backend-sql:+job-index-ddl+)))
  (ok (some (lambda (s) (search "(state, priority, submit_seq)" s))
            task-backend-sql:+job-index-ddl+)))

(deftest queue-revision-metadata
  (let ((p (task-backend-sql:queue-revision-plist)))
    (ok (equal "0002" (getf p :id)))
    (ok (equal "job-queue" (getf p :name)))
    (ok (equal "0001" (getf p :down-revision)))
    (ok (find task-backend-sql:+job-ddl+ (getf p :upgrade-sql) :test #'equal))
    (ok (find task-backend-sql:+resource-class-ddl+ (getf p :upgrade-sql)
              :test #'equal))
    (ok (every (lambda (s) (find s (getf p :upgrade-sql) :test #'equal))
               task-backend-sql:+job-index-ddl+))
    (ok (= 5 (length (getf p :downgrade-sql))))))

(deftest ensure-queue-schema-is-idempotent
  (let ((c (%connection)))
    (ok (eq c (task-backend-sql:ensure-queue-schema c)))
    (ok (eq c (task-backend-sql:ensure-queue-schema c)))
    (let ((names (%schema-names c)))
      (dolist (n +queue-schema-names+)
        (ok (member n names :test #'string=) n)))))

(deftest make-sql-job-queue-refuses-other-dialects
  (ok (typep (%catch (lambda ()
                       (task-backend-sql:make-sql-job-queue
                        :connection (%connection) :dialect :postgres)))
             'stack-task:task-error))
  (ok (typep (%catch (lambda ()
                       (task-backend-sql:make-sql-job-queue :connection nil)))
             'type-error)))

(deftest postgres-claim-skip-locked
  ;; TODO (README, queue.lisp header): the Postgres claim with
  ;; SELECT … FOR UPDATE SKIP LOCKED is not implemented; 0.2.0 runs on
  ;; SQLite only, so there is nothing to run here.
  (skip "Postgres SKIP LOCKED claim is a TODO; SQLite only in 0.2.0"))

;;; Column codec (given)

(deftest encode-value-roundtrip
  (dolist (v (list nil :render "index" '(:file "a.txt") '(1 "car") 1/2
                   0.25d0 -3 (vector 1 "two" :three) 'stack-task:job
                   '(:nested (:a 1) "x")))
    (ok (equalp v (task-backend-sql:decode-value
                   (task-backend-sql:encode-value v)))
        (prin1-to-string v)))
  (ok (equal "NIL" (task-backend-sql:encode-value nil)))
  (ok (null (task-backend-sql:decode-value nil)))
  (ok (typep (%catch (lambda ()
                       (task-backend-sql:decode-value "#.(error \"evaluated\")")))
             'reader-error)))

(deftest encode-value-hash-table-use-value
  (let ((h (make-hash-table :test 'equal)))
    (setf (gethash "a" h) 1)
    (ok (typep (%catch (lambda () (task-backend-sql:encode-value h)))
               'stack-task:task-serialization-error))
    (multiple-value-bind (text seen)
        (%call-handling 'stack-task:task-serialization-error
                        (lambda (c) (declare (ignore c))
                          (%invoke 'use-value "a=1"))
                        (lambda () (task-backend-sql:encode-value h)))
      (ok (= 1 (length seen)))
      (ok (equal "\"a=1\"" text)))))

(deftest job-events-sql-codec-roundtrip
  (let* ((p (make-instance 'stack-task:job-progress
                           :task-id "j-1" :seq 3 :phase "decode"
                           :fraction 1/2 :message "half way" :worker-id "w"
                           :timestamp 21))
         (tr (make-instance 'stack-task:job-transition
                            :task-id "j-1" :seq 2 :from-state :queued
                            :to-state :running :worker-id "w"
                            :reason :claimed :timestamp 20))
         (p2 (task-backend-sql:decode-event (task-backend-sql:encode-event p)))
         (tr2 (task-backend-sql:decode-event (task-backend-sql:encode-event tr))))
    (ok (stack-task:job-progress-p p2))
    (ok (equal '("decode" 1/2 "half way" "w" 3 "j-1" 21)
               (list (stack-task:job-progress-phase p2)
                     (stack-task:job-progress-fraction p2)
                     (stack-task:job-progress-message p2)
                     (stack-task:job-progress-worker-id p2)
                     (stack-task:event-seq p2)
                     (stack-task:event-task-id p2)
                     (stack-task:event-timestamp p2))))
    (ok (stack-task:job-transition-p tr2))
    (ok (equal '(:queued :running "w" :claimed 20)
               (list (stack-task:job-transition-from tr2)
                     (stack-task:job-transition-to tr2)
                     (stack-task:job-transition-worker-id tr2)
                     (stack-task:job-transition-reason tr2)
                     (stack-task:event-timestamp tr2))))))

;;; Queue on SQL (contract)

(deftest queue-migration-applies-to-empty-database
  (let ((c (%connection)))
    (dolist (sql (getf (task-backend-sql:journal-revision-plist) :upgrade-sql))
      (sql-protocol:execute c sql))
    (dolist (sql (getf (task-backend-sql:queue-revision-plist) :upgrade-sql))
      (sql-protocol:execute c sql))
    (let ((names (%schema-names c)))
      (dolist (n +queue-schema-names+)
        (ok (member n names :test #'string=) n)))
    (let* ((q (task-backend-sql:make-sql-job-queue
               :connection c :ensure-schema nil
               :resource-classes '((:one 1))))
           (id (stack-task:submit-job q :k :resource-class :one :now 1)))
      (ok (equal id (%claimed q "w" :now 2)))
      (ok (eql 1 (stack-task:resource-class-usage q :one))))
    (dolist (sql (getf (task-backend-sql:queue-revision-plist) :downgrade-sql))
      (sql-protocol:execute c sql))
    (let ((names (%schema-names c)))
      (ok (not (member "task_job" names :test #'string=)))
      (ok (not (member "task_resource_class" names :test #'string=)))
      (ok (member "task_journal" names :test #'string=)))))

(deftest sql-hash-table-payload-has-no-column-text
  (let ((q (%queue))
        (h (make-hash-table :test 'equal)))
    (setf (gethash "a" h) 1)
    (ok (typep (%catch (lambda () (stack-task:submit-job q :k :payload h)))
               'stack-task:task-serialization-error))
    (ok (null (stack-task:list-jobs q)))
    (let ((id (%call-handling 'stack-task:task-serialization-error
                              (lambda (c) (declare (ignore c))
                                (%invoke 'use-value '(("a" . 1))))
                              (lambda ()
                                (stack-task:submit-job q :k :payload h)))))
      (ok (equal '(("a" . 1))
                 (stack-task:job-payload (stack-task:job-status q id))))
      (ok (= 1 (length (stack-task:list-jobs q)))))))

(deftest sql-jobs-survive-reopen
  (%with-temp-db (path)
    (let (a b c)
      ;; First lifetime: submit, claim, report, request a cancel, exit.
      (let ((q1 (%file-queue path :resource-classes '((:heavy 1)))))
        (setf a (stack-task:submit-job q1 :render :payload '(:file "a")
                                          :resource-class :heavy :now 1)
              b (stack-task:submit-job q1 :render :resource-class :heavy :now 2)
              c (stack-task:submit-job q1 "index" :priority 5 :now 3))
        (ok (equal c (%claimed q1 "w1" :now 1000 :lease-seconds 10)))
        (ok (equal a (%claimed q1 "w1" :now 1000 :lease-seconds 10)))
        (stack-task:report-job-progress q1 a "w1" :phase "load" :fraction 1/2
                                                  :now 1001)
        (stack-task:cancel-job q1 c :reason "stop" :now 1002)
        (%disconnect (task-backend-sql:sql-job-queue-connection q1)))
      ;; Second lifetime: a new connection and queue, no classes given.
      (let ((q2 (%file-queue path)))
        (ok (equal '((:heavy 1)) (%class-pairs q2)))
        (ok (eql 1 (stack-task:resource-class-usage q2 :heavy)))
        (ok (equal (list a b c)
                   (mapcar #'stack-task:job-id (stack-task:list-jobs q2))))
        (let ((ja (stack-task:job-status q2 a)))
          (ok (eq :running (stack-task:job-state ja)))
          (ok (equal "w1" (stack-task:job-worker-id ja)))
          (ok (eql 1010 (stack-task:job-lease-until ja)))
          (ok (equal '(:file "a") (stack-task:job-payload ja)))
          (ok (eq :render (stack-task:job-kind ja)))
          (ok (eq :heavy (stack-task:job-resource-class ja))))
        (ok (equal "index" (stack-task:job-kind (stack-task:job-status q2 c))))
        (ok (eql 5 (stack-task:job-priority (stack-task:job-status q2 c))))
        (ok (stack-task:cancel-requested-p q2 c))
        (ok (equal '(1 2 3) (%event-seqs (stack-task:job-events q2 a))))
        (ok (eql 1/2 (stack-task:job-progress-fraction
                      (third (stack-task:job-events q2 a)))))
        ;; 1010 is not past the lease (strict <): heavy is full, c runs.
        (ok (null (stack-task:claim-job q2 "w2" :now 1010)))
        ;; At 1011 both leases expire: c (cancel requested) → :canceled,
        ;; a → :queued and is re-claimed ahead of b (FIFO kept).
        (ok (equal a (%claimed q2 "w2" :now 1011 :lease-seconds 10)))
        (ok (eq :canceled (%state q2 c)))
        (ok (equal "stop" (stack-task:job-failure (stack-task:job-status q2 c))))
        (ok (eql 2 (stack-task:job-attempts (stack-task:job-status q2 a))))
        (ok (eq :queued (%state q2 b)))
        (ok (null (stack-task:claim-job q2 "w2" :now 1011)))
        (ok (typep (%catch (lambda ()
                             (stack-task:complete-job q2 a "w1" :now 1012)))
                   'stack-task:job-lease-lost))
        (stack-task:complete-job q2 a "w2" :result :done :now 1013)
        (ok (equal b (%claimed q2 "w2" :now 1014)))
        (ok (equal '((nil :queued) (:queued :running) (:running :queued)
                     (:queued :running) (:running :completed))
                   (%transition-pairs (stack-task:job-events q2 a))))
        (ok (equal '(1 2 3 4 5 6) (%event-seqs (stack-task:job-events q2 a))))
        (let ((d (stack-task:submit-job q2 :render :now 1015)))
          (ok (not (member d (list a b c) :test #'equal)))
          (ok (equal (list a b c d)
                     (mapcar #'stack-task:job-id (stack-task:list-jobs q2)))))))))

(deftest sql-concurrent-connections-never-exceed-limits
  ;; Each worker thread has its own connection and queue object over one
  ;; SQLite file: admission is atomic in the database, not in the image.
  (%with-temp-db (path)
    (let* ((limits '((:heavy 2) (:light 1)))
           (counts '((:heavy 12) (:light 6) (nil 6)))
           (total (reduce #'+ counts :key #'second))
           (lock (bt:make-lock "sql admission test"))
           (active (list (cons :heavy 0) (cons :light 0) (cons nil 0)))
           (max-active (list (cons :heavy 0) (cons :light 0) (cons nil 0)))
           (max-usage (list (cons :heavy 0) (cons :light 0)))
           (claimed '())
           (workers '())
           (done 0)
           (deadline (+ (get-internal-real-time)
                        (* 60 internal-time-units-per-second))))
      (let ((setup (%file-queue path :resource-classes limits)))
        (loop for (class n) in counts
              do (loop repeat n
                       do (stack-task:submit-job setup :work
                                                 :resource-class class))))
      (let ((errors
              (%run-threads
               4
               (lambda (i)
                 (let ((q (%file-queue path :ensure-schema nil))
                       (worker (format nil "w~d" i)))
                   (loop while (and (< (bt:with-lock-held (lock) done) total)
                                    (< (get-internal-real-time) deadline))
                         do (let ((job (stack-task:claim-job q worker)))
                              (if (null job)
                                  (sleep 0.002)
                                  (let ((class (stack-task:job-resource-class job)))
                                    (bt:with-lock-held (lock)
                                      (push (stack-task:job-id job) claimed)
                                      (pushnew worker workers :test #'equal)
                                      (let ((n (incf (cdr (assoc class active)))))
                                        (setf (cdr (assoc class max-active))
                                              (max n (cdr (assoc class max-active))))))
                                    (when class
                                      (let ((u (stack-task:resource-class-usage
                                                q class)))
                                        (bt:with-lock-held (lock)
                                          (setf (cdr (assoc class max-usage))
                                                (max u (cdr (assoc class max-usage)))))))
                                    ;; Hold a :heavy job until a second one is
                                    ;; active (at most 2 s), so the overlap the
                                    ;; last check needs doesn't depend on how
                                    ;; fast a claim is (Windows runners: slower
                                    ;; than a fixed 10 ms hold).
                                    (if (eq class :heavy)
                                        (loop with until = (+ (get-internal-real-time)
                                                              (* 2 internal-time-units-per-second))
                                              until (or (>= (bt:with-lock-held (lock)
                                                              (cdr (assoc :heavy max-active)))
                                                            2)
                                                        (> (get-internal-real-time) until))
                                              do (sleep 0.002))
                                        (sleep 0.01))
                                    (bt:with-lock-held (lock)
                                      (decf (cdr (assoc class active))))
                                    (stack-task:complete-job
                                     q (stack-task:job-id job) worker :result i)
                                    (bt:with-lock-held (lock)
                                      (incf done))))))))
               :timeout 90)))
        (ok (null errors) (format nil "~{~a~^; ~}" errors))
        (ok (= total (length claimed)))
        (ok (= total (length (remove-duplicates claimed :test #'equal))))
        (ok (>= (length workers) 2))
        (let ((check (%file-queue path :ensure-schema nil)))
          (ok (= total (length (stack-task:list-jobs check :state :completed))))
          (ok (every (lambda (j) (eql 1 (stack-task:job-attempts j)))
                     (stack-task:list-jobs check)))
          (loop for (class limit) in limits
                do (ok (<= (cdr (assoc class max-active)) limit))
                   (ok (<= (cdr (assoc class max-usage)) limit))
                   (ok (eql 0 (stack-task:resource-class-usage check class)))))
        ;; The run really overlapped claims (else the bound proves nothing).
        (ok (>= (cdr (assoc :heavy max-active)) 2))))))

(deftest sql-subscription-delivers-after-commit
  (%with-temp-db (path)
    (let* ((q1 (%file-queue path))
           (q2 (%file-queue path :ensure-schema nil))
           (id (stack-task:submit-job q1 :k :now 1))
           (seen '()))
      (stack-task:claim-job q1 "w" :now 2 :lease-seconds 3600)
      (stack-task:subscribe-job-events
       q1 id
       (lambda (e)
         ;; Re-entrant read on the same queue, and a read on another
         ;; connection: both see the committed state.
         (push (list (stack-task:event-seq e)
                     (%state q1 id)
                     (%state q2 id))
               seen))
       :after-seq 2)
      (stack-task:report-job-progress q1 id "w" :phase "p" :now 3)
      (stack-task:complete-job q1 id "w" :result 1 :now 4)
      (ok (equal '((3 :running :running) (4 :completed :completed))
                 (reverse seen))))))

(deftest sql-other-connection-polls-events
  (%with-temp-db (path)
    (let* ((q1 (%file-queue path))
           (q2 (%file-queue path :ensure-schema nil))
           (id (stack-task:submit-job q1 :k :now 1)))
      (stack-task:claim-job q1 "w" :now 2 :lease-seconds 3600)
      (stack-task:report-job-progress q1 id "w" :phase "a" :now 3)
      (ok (equal '(2 3) (%event-seqs (stack-task:job-events q2 id :after-seq 1))))
      (multiple-value-bind (cb seen) (%collector)
        ;; A subscription on the other connection replays what is committed.
        (stack-task:subscribe-job-events q2 id cb)
        (ok (equal '(1 2 3) (%event-seqs (funcall seen)))))
      (stack-task:report-job-progress q1 id "w" :phase "b" :now 4)
      (let ((new (stack-task:job-events q2 id :after-seq 3)))
        (ok (equal '(4) (%event-seqs new)))
        (ok (equal "b" (stack-task:job-progress-phase (first new))))))))

(deftest sql-unwound-operation-commits-nothing
  (%with-temp-db (path)
    (let* ((q1 (%file-queue path :resource-classes '((:one 1))))
           ;; A short busy timeout: a transaction left open by Q1 would make
           ;; Q2's writes below fail instead of waiting.
           (q2 (%file-queue path :ensure-schema nil :busy-timeout 500))
           (id (stack-task:submit-job q1 :k :resource-class :one :now 1)))
      (stack-task:claim-job q1 "w" :now 2 :lease-seconds 3600)
      (ok (typep (%catch (lambda ()
                           (stack-task:complete-job q1 id "w"
                                                    :result (list #'car) :now 3)))
                 'stack-task:task-serialization-error))
      (ok (eq :none
              (%call-handling 'stack-task:job-not-found
                              (lambda (c) (declare (ignore c))
                                (%invoke 'use-value :none))
                              (lambda ()
                                (stack-task:complete-job q1 "missing" "w"
                                                         :now 3)))))
      (ok (eq :running (%state q2 id)))
      (ok (eql 1 (stack-task:resource-class-usage q2 :one)))
      (ok (= 2 (length (stack-task:job-events q2 id))))
      (ok (stringp (%catch-value (lambda () (stack-task:submit-job q2 :k :now 4)))))
      (ok (eq :completed
              (stack-task:job-state
               (stack-task:complete-job q1 id "w" :result 1 :now 5)))))))
