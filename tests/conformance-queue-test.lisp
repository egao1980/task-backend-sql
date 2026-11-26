(in-package #:task-backend-sql/tests)

;;; 0.2.0 conformance: task-protocol 0.3.0 tests/job-queue-test.lisp against
;;; SQL-JOB-QUEUE (see conformance-support.lisp). queue order, state machine, cancel, leases, journal.

(deftest submit-job-snapshot
  (let* ((q (%queue))
         (a (stack-task:submit-job q :render :payload '(:file "a.txt") :now 1000))
         (b (stack-task:submit-job q :render :now 1001))
         (c (stack-task:submit-job q :index :priority 3 :now 1002))
         (ja (stack-task:job-status q a)))
    (ok (every #'stringp (list a b c)))
    (ok (= 3 (length (remove-duplicates (list a b c) :test #'equal))))
    (ok (stack-task:job-p ja))
    (ok (equal a (stack-task:job-id ja)))
    (ok (eq :render (stack-task:job-kind ja)))
    (ok (equal '(:file "a.txt") (stack-task:job-payload ja)))
    (ok (eql 0 (stack-task:job-priority ja)))
    (ok (eql 3 (stack-task:job-priority (stack-task:job-status q c))))
    (ok (null (stack-task:job-resource-class ja)))
    (ok (eq :queued (stack-task:job-state ja)))
    (ok (eql 1000 (stack-task:job-submitted-at ja)))
    (ok (eql 1000 (stack-task:job-updated-at ja)))
    (ok (eql 0 (stack-task:job-attempts ja)))
    (ok (null (stack-task:job-worker-id ja)))
    (ok (null (stack-task:job-lease-until ja)))
    (ok (null (stack-task:job-cancel-requested ja)))
    (ng (eq ja (stack-task:job-status q a)))))

(deftest claim-fifo-within-priority
  (let* ((q (%queue))
         (ids (loop for i below 5
                    collect (stack-task:submit-job q :k :payload i :now (+ 1000 i)))))
    (ok (equal ids
               (loop repeat 5 collect (%claimed q "w" :now 2000))))
    (ok (null (stack-task:claim-job q "w" :now 2000)))))

(deftest claim-priority-then-fifo
  (let* ((q (%queue))
         (low (stack-task:submit-job q :k :priority 0 :now 1))
         (high1 (stack-task:submit-job q :k :priority 5 :now 2))
         (mid (stack-task:submit-job q :k :priority 2 :now 3))
         (high2 (stack-task:submit-job q :k :priority 5 :now 4))
         (neg (stack-task:submit-job q :k :priority -1 :now 5)))
    (ok (equal (list high1 high2 mid low neg)
               (loop repeat 5 collect (%claimed q "w" :now 10))))))

(deftest claim-sets-running-fields
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 900))
         (job (stack-task:claim-job q "w1" :now 1000 :lease-seconds 10)))
    (ok (equal id (stack-task:job-id job)))
    (ok (eq :running (stack-task:job-state job)))
    (ok (equal "w1" (stack-task:job-worker-id job)))
    (ok (eql 1000 (stack-task:job-started-at job)))
    (ok (eql 1010 (stack-task:job-lease-until job)))
    (ok (eql 1 (stack-task:job-attempts job)))
    (ok (eq :running (%state q id)))
    (ok (null (stack-task:claim-job q "w2" :now 1000)))))

(deftest claim-default-lease-is-30-seconds
  (let ((q (%queue)))
    (stack-task:submit-job q :k :now 1)
    (ok (eql 130 (stack-task:job-lease-until
                  (stack-task:claim-job q "w" :now 100))))))

(deftest claim-kinds-filter
  (let* ((q (%queue))
         (render (stack-task:submit-job q :render :now 1))
         (index (stack-task:submit-job q "index" :now 2)))
    (ok (null (stack-task:claim-job q "w" :kinds '(:other) :now 3)))
    (ok (equal index (%claimed q "w" :kinds '("index") :now 3)))
    (ok (equal render (%claimed q "w" :kinds '(:render :other) :now 3)))))

(deftest complete-job-terminal
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (let ((job (stack-task:complete-job q id "w" :result '(:pages 12) :now 5)))
      (ok (eq :completed (stack-task:job-state job)))
      (ok (equal '(:pages 12) (stack-task:job-result job)))
      (ok (eql 5 (stack-task:job-finished-at job)))
      (ok (eql 5 (stack-task:job-updated-at job)))
      (ok (null (stack-task:job-worker-id job)))
      (ok (null (stack-task:job-lease-until job))))
    (ok (equal '(:pages 12) (stack-task:job-result (stack-task:job-status q id))))
    (ok (null (stack-task:claim-job q "w" :now 6)))))

(deftest fail-job-stores-error-text
  (let* ((q (%queue))
         (a (stack-task:submit-job q :k :now 1))
         (b (stack-task:submit-job q :k :now 2)))
    (stack-task:claim-job q "w" :now 3)
    (stack-task:claim-job q "w" :now 3)
    (let ((ja (stack-task:fail-job q a "w"
                                   :error (make-condition
                                           'simple-error
                                           :format-control "boom ~a"
                                           :format-arguments '(7))
                                   :now 4))
          (jb (stack-task:fail-job q b "w" :error "out of memory" :now 4)))
      (ok (eq :failed (stack-task:job-state ja)))
      (ok (equal "boom 7" (stack-task:job-failure ja)))
      (ok (equal "out of memory" (stack-task:job-failure jb)))
      (ok (eql 4 (stack-task:job-finished-at ja)))
      (ok (null (stack-task:job-worker-id ja))))))

(deftest list-jobs-filters
  (let* ((q (%queue '(:heavy 5)))
         (a (stack-task:submit-job q :render :resource-class :heavy :now 1))
         (b (stack-task:submit-job q :index :now 2))
         (c (stack-task:submit-job q :render :now 3))
         (d (stack-task:submit-job q :render :now 4)))
    (stack-task:claim-job q "w" :now 5)          ; a
    (stack-task:claim-job q "w" :now 5)          ; b
    (stack-task:complete-job q b "w" :now 6)
    (stack-task:cancel-job q d :now 7)
    (flet ((ids (&rest keys)
             (mapcar #'stack-task:job-id (apply #'stack-task:list-jobs q keys))))
      (ok (equal (list a b c d) (ids)))
      (ok (equal (list c) (ids :state :queued)))
      (ok (equal (list a b) (ids :state '(:running :completed))))
      (ok (equal (list a c d) (ids :kind :render)))
      (ok (equal (list c) (ids :kind :render :state :queued)))
      (ok (equal (list a) (ids :resource-class :heavy)))
      (ok (null (ids :kind :nothing))))))

(deftest cancel-queued-job
  (let* ((q (%queue))
         (a (stack-task:submit-job q :k :now 1))
         (b (stack-task:submit-job q :k :now 2)))
    (let ((ja (stack-task:cancel-job q a :now 3))
          (jb (stack-task:cancel-job q b :reason "user" :now 3)))
      (ok (eq :canceled (stack-task:job-state ja)))
      (ok (eq :canceled (stack-task:job-failure ja)))
      (ok (equal "user" (stack-task:job-failure jb)))
      (ok (eql 3 (stack-task:job-finished-at ja))))
    (ok (stack-task:cancel-requested-p q a))
    (ok (null (stack-task:claim-job q "w" :now 4)))
    (ok (eq :canceled (stack-task:job-state (stack-task:cancel-job q a :now 5))))
    (ok (eql 3 (stack-task:job-finished-at (stack-task:job-status q a))))))

(deftest cancel-running-is-cooperative
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (ok (null (stack-task:cancel-requested-p q id)))
    (let ((job (stack-task:cancel-job q id :reason "stop" :now 3)))
      (ok (eq :running (stack-task:job-state job)))
      (ok (eq t (stack-task:job-cancel-requested job)))
      (ok (equal "w" (stack-task:job-worker-id job))))
    (ok (stack-task:cancel-requested-p q id))
    (ok (stack-task:job-cancel-requested
         (stack-task:heartbeat-job q id "w" :now 4)))
    (ok (eq :running (stack-task:job-state (stack-task:cancel-job q id :now 4))))
    (let ((job (stack-task:acknowledge-cancel q id "w" :now 5)))
      (ok (eq :canceled (stack-task:job-state job)))
      (ok (equal "stop" (stack-task:job-failure job)))
      (ok (eql 5 (stack-task:job-finished-at job)))
      (ok (null (stack-task:job-worker-id job))))
    (ok (stack-task:cancel-requested-p q id))))

(deftest acknowledge-cancel-without-request
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (let ((job (stack-task:acknowledge-cancel q id "w" :now 3)))
      (ok (eq :canceled (stack-task:job-state job)))
      (ok (eq :canceled (stack-task:job-failure job))))))

(deftest complete-wins-over-cancel-request
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (stack-task:cancel-job q id :now 3)
    (ok (eq :completed
            (stack-task:job-state
             (stack-task:complete-job q id "w" :result 1 :now 4))))))

(deftest cancel-force-running
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (let ((job (stack-task:cancel-job q id :force t :reason "killed" :now 3)))
      (ok (eq :canceled (stack-task:job-state job)))
      (ok (equal "killed" (stack-task:job-failure job)))
      (ok (null (stack-task:job-worker-id job)))
      (ok (null (stack-task:job-lease-until job))))
    (ok (typep (%catch (lambda () (stack-task:complete-job q id "w" :now 4)))
               'stack-task:job-state-error))
    (ok (eq :canceled (%state q id)))))

(deftest heartbeat-extends-lease
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 1000 :lease-seconds 10)
    (ok (eql 1015 (stack-task:job-lease-until
                   (stack-task:heartbeat-job q id "w" :now 1005 :lease-seconds 10))))
    (ok (eql 1035 (stack-task:job-lease-until
                   (stack-task:heartbeat-job q id "w" :now 1005))))
    (ok (null (stack-task:expire-job-leases q :now 1035)))
    (ok (eq :running (%state q id)))))

(deftest lease-expiry-requeues
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w1" :now 1000 :lease-seconds 10)
    (ok (null (stack-task:expire-job-leases q :now 1010)))
    (ok (null (stack-task:claim-job q "w2" :now 1010)))
    (let ((job (stack-task:claim-job q "w2" :now 1011 :lease-seconds 10)))
      (ok (equal id (stack-task:job-id job)))
      (ok (equal "w2" (stack-task:job-worker-id job)))
      (ok (eql 2 (stack-task:job-attempts job)))
      (ok (eql 1021 (stack-task:job-lease-until job))))
    (let ((c (%catch (lambda () (stack-task:complete-job q id "w1" :now 1012)))))
      (ok (typep c 'stack-task:job-lease-lost))
      (ok (equal "w1" (and c (stack-task:job-lease-lost-worker-id c))))
      (ok (equal "w2" (and c (stack-task:job-lease-lost-owner c))))
      (ok (eq :complete (and c (stack-task:job-state-error-operation c)))))
    (ok (eq :completed
            (stack-task:job-state (stack-task:complete-job q id "w2" :now 1013))))))

(deftest expire-job-leases-explicit
  (let* ((q (%queue))
         (a (stack-task:submit-job q :k :now 1))
         (b (stack-task:submit-job q :k :now 2))
         (c (stack-task:submit-job q :k :now 3)))
    (stack-task:claim-job q "w" :now 100 :lease-seconds 5)   ; a, until 105
    (stack-task:claim-job q "w" :now 100 :lease-seconds 50)  ; b, until 150
    (stack-task:claim-job q "w" :now 100 :lease-seconds 5)   ; c, until 105
    (stack-task:cancel-job q c :reason "bye" :now 101)
    (let ((changed (stack-task:expire-job-leases q :now 106)))
      (ok (equal (list a c) (mapcar #'stack-task:job-id changed)))
      (ok (equal '(:queued :canceled) (mapcar #'stack-task:job-state changed))))
    (ok (eq :queued (%state q a)))
    (ok (null (stack-task:job-worker-id (stack-task:job-status q a))))
    (ok (null (stack-task:job-lease-until (stack-task:job-status q a))))
    (ok (eql 1 (stack-task:job-attempts (stack-task:job-status q a))))
    (ok (eq :running (%state q b)))
    (ok (eq :canceled (%state q c)))
    (ok (equal "bye" (stack-task:job-failure (stack-task:job-status q c))))
    (ok (null (stack-task:expire-job-leases q :now 106)))))

(deftest lease-expiry-keeps-fifo-position
  (let* ((q (%queue))
         (a (stack-task:submit-job q :k :now 1))
         (b (stack-task:submit-job q :k :now 2)))
    (stack-task:claim-job q "w1" :now 100 :lease-seconds 5)
    (ok (equal a (%claimed q "w2" :now 200)))
    (ok (equal b (%claimed q "w2" :now 200)))))

(deftest job-journal-lifecycle
  ;; SQL: the journal is a SQL-TASK-JOURNAL on the queue's connection.
  (let* ((conn (%connection))
         (journal (task-backend-sql:make-sql-task-journal :connection conn))
         (q (task-backend-sql:make-sql-job-queue :connection conn
                                                 :journal journal))
         (id (stack-task:submit-job q :k :now 10)))
    (ok (eq journal (stack-task:job-queue-journal q)))
    (stack-task:claim-job q "w" :now 20)
    (stack-task:report-job-progress q id "w" :phase "load" :fraction 1/4 :now 21)
    (stack-task:report-job-progress q id "w" :phase "run" :fraction 3/4 :now 22)
    (stack-task:complete-job q id "w" :result t :now 30)
    (let ((events (stack-task:journal-events journal id)))
      (ok (equal '(1 2 3 4 5) (%event-seqs events)))
      (ok (every (lambda (e) (equal id (stack-task:event-task-id e))) events))
      (ok (equal '(10 20 21 22 30) (mapcar #'stack-task:event-timestamp events)))
      (ok (equal '((nil :queued) (:queued :running) (:running :completed))
                 (%transition-pairs events)))
      (ok (equal '(:job-transition :job-transition :job-progress :job-progress
                   :job-transition)
                 (mapcar #'stack-task:event-type-keyword events)))
      (ok (equal '(:submitted :claimed :completed)
                 (loop for e in events
                       when (stack-task:job-transition-p e)
                         collect (stack-task:job-transition-reason e))))
      (ok (equal "w" (stack-task:job-transition-worker-id (second events))))
      (ok (equal (%event-seqs events)
                 (%event-seqs (stack-task:job-events q id)))))))

(deftest journal-transition-reasons
  (let* ((q (%queue))
         (a (stack-task:submit-job q :k :now 1))
         (b (stack-task:submit-job q :k :now 2))
         (c (stack-task:submit-job q :k :now 3))
         (d (stack-task:submit-job q :k :now 4))
         (e (stack-task:submit-job q :k :now 5)))
    (stack-task:cancel-job q a :now 6)
    (stack-task:claim-job q "w" :now 10 :lease-seconds 100)   ; b
    (stack-task:claim-job q "w" :now 10 :lease-seconds 100)   ; c
    (stack-task:claim-job q "w" :now 10 :lease-seconds 1)     ; d
    (stack-task:claim-job q "w" :now 10 :lease-seconds 100)   ; e
    (stack-task:fail-job q b "w" :error "x" :now 12)
    (stack-task:cancel-job q c :force t :now 12)
    (stack-task:expire-job-leases q :now 12)
    (stack-task:cancel-job q e :now 12)                       ; no event
    (stack-task:acknowledge-cancel q e "w" :now 13)
    (flet ((last-transition (id)
             (let ((ev (car (last (remove-if-not #'stack-task:job-transition-p
                                                 (stack-task:job-events q id))))))
               (list (stack-task:job-transition-from ev)
                     (stack-task:job-transition-to ev)
                     (stack-task:job-transition-reason ev)))))
      (ok (equal '(:queued :canceled :canceled) (last-transition a)))
      (ok (equal '(:running :failed :failed) (last-transition b)))
      (ok (equal '(:running :canceled :forced) (last-transition c)))
      (ok (equal '(:running :queued :lease-expired) (last-transition d)))
      (ok (equal '(:running :canceled :canceled) (last-transition e)))
      (ok (= 3 (length (stack-task:job-events q e)))))))

(deftest job-journal-replays-into-durable-task
  ;; SQL: replayed from a SQL-TASK-JOURNAL.
  (let ((journal (task-backend-sql:make-sql-task-journal
                  :connection (%connection)))
        (task (stack-task:make-durable-task :id "job-r")))
    (stack-task:append-event journal (make-instance 'stack-task:task-started
                                                    :task-id "job-r"))
    (stack-task:append-event journal (make-instance 'stack-task:job-transition
                                                    :task-id "job-r"
                                                    :from-state :queued
                                                    :to-state :running))
    (stack-task:append-event journal (make-instance 'stack-task:job-progress
                                                    :task-id "job-r"
                                                    :fraction 1/2))
    (stack-task:replay-journal journal task)
    (ok (eq :running (stack-task:durable-task-status task)))))
