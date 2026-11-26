(in-package #:task-backend-sql/tests)

;;; 0.2.0 conformance: task-protocol 0.3.0 tests/job-conditions-test.lisp against
;;; SQL-JOB-QUEUE (see conformance-support.lisp). job conditions and their restarts.

(deftest job-not-found-use-value
  (let ((q (%queue)))
    (let ((c (%catch (lambda () (stack-task:job-status q "missing")))))
      (ok (typep c 'stack-task:job-not-found))
      (ok (equal "missing" (and c (stack-task:job-error-job-id c)))))
    (multiple-value-bind (value seen)
        (%call-handling 'stack-task:job-not-found
                        (lambda (c) (declare (ignore c)) (%invoke 'use-value :none))
                        (lambda () (stack-task:job-status q "missing")))
      (ok (eq :none value))
      (ok (= 1 (length seen))))))

(defparameter *job-id-operations*
  (list (list "cancel-job" (lambda (q id) (stack-task:cancel-job q id)))
        (list "cancel-requested-p" (lambda (q id) (stack-task:cancel-requested-p q id)))
        (list "heartbeat-job" (lambda (q id) (stack-task:heartbeat-job q id "w")))
        (list "complete-job" (lambda (q id) (stack-task:complete-job q id "w")))
        (list "fail-job" (lambda (q id) (stack-task:fail-job q id "w")))
        (list "acknowledge-cancel" (lambda (q id) (stack-task:acknowledge-cancel q id "w")))
        (list "report-job-progress"
              (lambda (q id) (stack-task:report-job-progress q id "w" :fraction 0)))
        (list "job-events" (lambda (q id) (stack-task:job-events q id)))
        (list "subscribe-job-events"
              (lambda (q id) (stack-task:subscribe-job-events q id #'identity)))
        (list "append-job-event"
              (lambda (q id)
                (stack-task:append-job-event
                 q id (make-instance 'stack-task:wait-input))))))

(deftest-parametrize unknown-job-id-signals-not-found
    ((name op) :rows *job-id-operations*)
  (let ((q (%queue)))
    (stack-task:submit-job q :k)
    (let ((c (%catch (lambda () (funcall op q "missing"))))
          (value (%catch-value
                  (lambda ()
                    (%call-handling 'stack-task:job-not-found
                                    (lambda (c) (declare (ignore c))
                                      (%invoke 'use-value :substitute))
                                    (lambda () (funcall op q "missing")))))))
      (ok (typep c 'stack-task:job-not-found) name)
      (ok (eq :substitute value) name))))

(deftest unknown-resource-class-use-value
  (let ((q (%queue '(:light 2))))
    (let ((c (%catch (lambda () (stack-task:submit-job q :k :resource-class :gpu)))))
      (ok (typep c 'stack-task:unknown-resource-class))
      (ok (eq :gpu (and c (stack-task:unknown-resource-class-name c)))))
    (ok (null (stack-task:list-jobs q)))
    (let ((id (%call-handling 'stack-task:unknown-resource-class
                              (lambda (c) (declare (ignore c))
                                (%invoke 'use-value :light))
                              (lambda ()
                                (stack-task:submit-job q :k :resource-class :gpu)))))
      (ok (eq :light (stack-task:job-resource-class (stack-task:job-status q id)))))
    (let ((id (%call-handling 'stack-task:unknown-resource-class
                              (lambda (c) (declare (ignore c))
                                (%invoke 'use-value nil))
                              (lambda ()
                                (stack-task:submit-job q :k :resource-class :gpu)))))
      (ok (null (stack-task:job-resource-class (stack-task:job-status q id)))))
    ;; The substitute is checked again.
    (let ((names '(:also-unknown :light)))
      (multiple-value-bind (id seen)
          (%call-handling 'stack-task:unknown-resource-class
                          (lambda (c) (declare (ignore c))
                            (%invoke 'use-value (pop names)))
                          (lambda ()
                            (stack-task:submit-job q :k :resource-class :gpu)))
        (ok (= 2 (length seen)))
        (ok (equal '(:gpu :also-unknown)
                   (mapcar #'stack-task:unknown-resource-class-name seen)))
        (ok (eq :light (stack-task:job-resource-class
                        (stack-task:job-status q id))))))
    (ok (= 3 (length (stack-task:list-jobs q))))))

(deftest job-state-error-continue
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (let ((c (%catch (lambda () (stack-task:complete-job q id "w")))))
      (ok (typep c 'stack-task:job-state-error))
      (ok (equal id (and c (stack-task:job-error-job-id c))))
      (ok (eq :queued (and c (stack-task:job-state-error-state c))))
      (ok (eq :complete (and c (stack-task:job-state-error-operation c)))))
    (multiple-value-bind (job seen)
        (%call-handling 'stack-task:job-state-error
                        (lambda (c) (declare (ignore c)) (%invoke 'continue))
                        (lambda () (stack-task:fail-job q id "w" :error "x")))
      (ok (= 1 (length seen)))
      (ok (stack-task:job-p job))
      (ok (eq :queued (and job (stack-task:job-state job)))))
    (ok (eq :queued (%state q id)))
    (ok (= 1 (length (stack-task:job-events q id))))))

(deftest cancel-finished-job-state-error
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (stack-task:complete-job q id "w" :now 3)
    (let ((c (%catch (lambda () (stack-task:cancel-job q id)))))
      (ok (typep c 'stack-task:job-state-error))
      (ok (eq :completed (and c (stack-task:job-state-error-state c))))
      (ok (eq :cancel (and c (stack-task:job-state-error-operation c)))))
    (let ((job (%call-handling 'stack-task:job-state-error
                               (lambda (c) (declare (ignore c)) (%invoke 'continue))
                               (lambda () (stack-task:cancel-job q id)))))
      (ok (eq :completed (stack-task:job-state job))))))

(deftest job-lease-lost-continue
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "owner" :now 100 :lease-seconds 10)
    (multiple-value-bind (job seen)
        (%call-handling 'stack-task:job-lease-lost
                        (lambda (c) (declare (ignore c)) (%invoke 'continue))
                        (lambda ()
                          (stack-task:heartbeat-job q id "intruder" :now 105
                                                    :lease-seconds 100)))
      (ok (= 1 (length seen)))
      (ok (equal '("intruder" "owner" :heartbeat :running)
                 (let ((c (first seen)))
                   (and c (list (stack-task:job-lease-lost-worker-id c)
                                (stack-task:job-lease-lost-owner c)
                                (stack-task:job-state-error-operation c)
                                (stack-task:job-state-error-state c))))))
      (ok (eql 110 (and job (stack-task:job-lease-until job)))))
    (ok (equal "owner" (stack-task:job-worker-id (stack-task:job-status q id))))))

(deftest progress-state-error-continue-returns-nil
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (multiple-value-bind (value seen)
        (%call-handling 'stack-task:job-state-error
                        (lambda (c) (declare (ignore c)) (%invoke 'continue))
                        (lambda ()
                          (stack-task:report-job-progress q id "w" :fraction 0)))
      (ok (= 1 (length seen)))
      (ok (null value)))
    (ok (= 1 (length (stack-task:job-events q id))))))

(deftest progress-error-use-value
  (let* ((q (%queue))
         (id (stack-task:submit-job q :k :now 1)))
    (stack-task:claim-job q "w" :now 2)
    (let ((c (%catch (lambda ()
                       (stack-task:report-job-progress q id "w" :fraction 3/2)))))
      (ok (typep c 'stack-task:job-progress-error))
      (ok (eql 3/2 (and c (stack-task:job-progress-error-fraction c))))
      (ok (equal id (and c (stack-task:job-error-job-id c)))))
    (let ((fractions (list 7 1/2)))
      (multiple-value-bind (ev seen)
          (%call-handling 'stack-task:job-progress-error
                          (lambda (c) (declare (ignore c))
                            (%invoke 'use-value (pop fractions)))
                          (lambda ()
                            (stack-task:report-job-progress q id "w" :fraction 3/2)))
        (ok (equal '(3/2 7) (mapcar #'stack-task:job-progress-error-fraction seen)))
        (ok (eql 1/2 (and ev (stack-task:job-progress-fraction ev))))))
    (ok (= 3 (length (stack-task:job-events q id))))))

(deftest payload-and-result-serialization
  (let ((q (%queue)))
    (ok (typep (%catch (lambda () (stack-task:submit-job q :k :payload #'car)))
               'stack-task:task-serialization-error))
    (ok (null (stack-task:list-jobs q)))
    (let ((id (%call-handling 'stack-task:task-serialization-error
                              (lambda (c) (declare (ignore c))
                                (%invoke 'use-value "car"))
                              (lambda ()
                                (stack-task:submit-job q :k :payload (list 1 #'car))))))
      (ok (equal '(1 "car") (stack-task:job-payload (stack-task:job-status q id))))
      (stack-task:claim-job q "w")
      (ok (typep (%catch (lambda ()
                           (stack-task:complete-job q id "w" :result (list #'car))))
                 'stack-task:task-serialization-error))
      (ok (eq :running (%state q id)))
      (let ((job (%call-handling 'stack-task:task-serialization-error
                                 (lambda (c) (declare (ignore c))
                                   (%invoke 'use-value :fn))
                                 (lambda ()
                                   (stack-task:complete-job q id "w"
                                                            :result (list #'car))))))
        (ok (equal '(:fn) (stack-task:job-result job)))))))

(deftest submit-priority-type
  (let ((q (%queue)))
    (ok (typep (%catch (lambda () (stack-task:submit-job q :k :priority "high")))
               'type-error))
    (ok (typep (%catch (lambda () (stack-task:submit-job q :k :priority 1.5)))
               'type-error))
    (ok (null (stack-task:list-jobs q)))))
