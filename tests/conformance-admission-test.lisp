(in-package #:task-backend-sql/tests)

;;; 0.2.0 conformance: task-protocol 0.3.0 tests/job-admission-test.lisp against
;;; SQL-JOB-QUEUE (see conformance-support.lisp). resource classes and admission at claim time.

(deftest resource-class-registry
  (let* ((q (%queue))
         (rc (stack-task:define-resource-class q :heap-4g :limit 3)))
    (ok (stack-task:resource-class-p rc))
    (ok (eq :heap-4g (stack-task:resource-class-name rc)))
    (ok (eql 3 (stack-task:resource-class-limit rc)))
    (stack-task:define-resource-class q :gpu :limit 1)
    (ok (equal '(:heap-4g :gpu)
               (mapcar #'stack-task:resource-class-name
                       (stack-task:list-resource-classes q))))
    (stack-task:define-resource-class q :heap-4g :limit 5)
    (ok (eql 5 (stack-task:resource-class-limit
                (stack-task:find-resource-class q :heap-4g))))
    (ok (equal '(:heap-4g :gpu)
               (mapcar #'stack-task:resource-class-name
                       (stack-task:list-resource-classes q))))
    (ok (null (stack-task:find-resource-class q :nope)))
    (ok (eql 0 (stack-task:resource-class-usage q :gpu)))
    (ok (eql 0 (stack-task:resource-class-usage q :nope)))))

(deftest resource-class-limit-type
  (let ((q (%queue)))
    (ok (typep (%catch (lambda ()
                         (stack-task:define-resource-class q :x :limit -1)))
               'type-error))
    (ok (typep (%catch (lambda ()
                         (stack-task:define-resource-class q :x :limit "3")))
               'type-error))
    (ok (eql 0 (stack-task:resource-class-limit
                (stack-task:define-resource-class q :x :limit 0))))))

(deftest constructor-defines-resource-classes
  (let ((q (%queue '(:heavy 2) '(:light 7))))
    (ok (equal '((:heavy 2) (:light 7))
               (mapcar (lambda (rc)
                         (list (stack-task:resource-class-name rc)
                               (stack-task:resource-class-limit rc)))
                       (stack-task:list-resource-classes q))))))

(deftest admission-skips-full-class
  (let* ((q (%queue '(:heavy 2)))
         (h1 (stack-task:submit-job q :k :resource-class :heavy :now 1))
         (h2 (stack-task:submit-job q :k :resource-class :heavy :now 2))
         (h3 (stack-task:submit-job q :k :resource-class :heavy :now 3))
         (free (stack-task:submit-job q :k :now 4)))
    (ok (equal (list h1 h2 free)
               (loop repeat 3 collect (%claimed q "w" :now 10))))
    (ok (eql 2 (stack-task:resource-class-usage q :heavy)))
    (ok (null (stack-task:claim-job q "w" :now 10)))
    (ok (eq :queued (%state q h3)))
    (stack-task:complete-job q h1 "w" :now 11)
    (ok (eql 1 (stack-task:resource-class-usage q :heavy)))
    (ok (equal h3 (%claimed q "w" :now 12)))
    (ok (eql 2 (stack-task:resource-class-usage q :heavy)))))

(deftest admission-priority-within-free-classes
  (let* ((q (%queue '(:gpu 1) '(:cpu 4)))
         (g1 (stack-task:submit-job q :k :resource-class :gpu :priority 9 :now 1))
         (g2 (stack-task:submit-job q :k :resource-class :gpu :priority 9 :now 2))
         (c1 (stack-task:submit-job q :k :resource-class :cpu :priority 1 :now 3)))
    (ok (equal g1 (%claimed q "w" :now 5)))
    (ok (equal c1 (%claimed q "w" :now 5)))
    (ok (null (stack-task:claim-job q "w" :now 5)))
    (stack-task:fail-job q g1 "w" :error "x" :now 6)
    (ok (equal g2 (%claimed q "w" :now 7)))))

(deftest admission-limit-zero-pauses
  (let* ((q (%queue '(:paused 0)))
         (id (stack-task:submit-job q :k :resource-class :paused :now 1)))
    (ok (null (stack-task:claim-job q "w" :now 2)))
    (stack-task:define-resource-class q :paused :limit 1)
    (ok (equal id (%claimed q "w" :now 3)))))

(deftest lowering-limit-preempts-nothing
  (let* ((q (%queue '(:heavy 2)))
         (a (stack-task:submit-job q :k :resource-class :heavy :now 1))
         (b (stack-task:submit-job q :k :resource-class :heavy :now 2))
         (c (stack-task:submit-job q :k :resource-class :heavy :now 3)))
    (stack-task:claim-job q "w" :now 4)
    (stack-task:claim-job q "w" :now 4)
    (stack-task:define-resource-class q :heavy :limit 1)
    (ok (eql 2 (stack-task:resource-class-usage q :heavy)))
    (ok (equal '(:running :running) (list (%state q a) (%state q b))))
    (ok (null (stack-task:claim-job q "w" :now 5)))
    (stack-task:complete-job q a "w" :now 6)
    (ok (null (stack-task:claim-job q "w" :now 7)))
    (stack-task:complete-job q b "w" :now 8)
    (ok (equal c (%claimed q "w" :now 9)))))

(defun %release (how q id)
  "Make the running job ID (claimed by \"w\" at 1000, lease 10) leave
   :running by HOW. → the NOW to claim at next."
  (ecase how
    (:complete (stack-task:complete-job q id "w" :now 1001) 1001)
    (:fail (stack-task:fail-job q id "w" :error "x" :now 1001) 1001)
    (:acknowledge-cancel
     (stack-task:cancel-job q id :now 1001)
     (stack-task:acknowledge-cancel q id "w" :now 1001)
     1001)
    (:force-cancel (stack-task:cancel-job q id :force t :now 1001) 1001)
    (:lease-expiry (stack-task:expire-job-leases q :now 1011) 1011)
    (:lease-expiry-in-claim 1011)
    (:lease-expiry-canceled
     (stack-task:cancel-job q id :now 1001)
     (stack-task:expire-job-leases q :now 1011)
     1011)))

(deftest-parametrize slot-released-on-every-path
    ((how expected-state)
     :ids ("complete" "fail" "acknowledge-cancel" "force-cancel"
           "lease-expiry" "lease-expiry-in-claim" "lease-expiry-canceled")
     (:complete :completed)
     (:fail :failed)
     (:acknowledge-cancel :canceled)
     (:force-cancel :canceled)
     (:lease-expiry :queued)
     (:lease-expiry-in-claim nil)
     (:lease-expiry-canceled :canceled))
  (let* ((q (%queue '(:one 1)))
         (j1 (stack-task:submit-job q :k :resource-class :one :now 1))
         (j2 (stack-task:submit-job q :k :resource-class :one :now 2)))
    (ok (equal j1 (%claimed q "w" :now 1000 :lease-seconds 10)))
    (ok (null (stack-task:claim-job q "w2" :now 1000)))
    (ok (eql 1 (stack-task:resource-class-usage q :one)))
    (let ((now (%release how q j1)))
      (when expected-state
        (ok (eq expected-state (%state q j1)))
        (ok (eql 0 (stack-task:resource-class-usage q :one))))
      (let ((next (%claimed q "w2" :now now)))
        (ok (member next (list j1 j2) :test #'equal))
        (when (member how '(:lease-expiry :lease-expiry-in-claim))
          (ok (equal j1 next)))
        (when (member how '(:complete :fail :acknowledge-cancel :force-cancel
                            :lease-expiry-canceled))
          (ok (equal j2 next)))))
    (ok (eql 1 (stack-task:resource-class-usage q :one)))
    (ok (null (stack-task:claim-job q "w3" :now 1011)))))

(deftest concurrent-claims-never-exceed-limits
  (let* ((limits '((:heavy 3) (:light 1)))
         (q (apply #'%queue limits))
         (counts '((:heavy 16) (:light 8)))
         (total (reduce #'+ counts :key #'second))
         (lock (bt:make-lock "admission test"))
         (active (list (cons :heavy 0) (cons :light 0)))
         (max-active (list (cons :heavy 0) (cons :light 0)))
         (max-usage (list (cons :heavy 0) (cons :light 0)))
         (claimed '())
         (done 0)
         (deadline (+ (get-internal-real-time)
                      (* 30 internal-time-units-per-second))))
    (loop for (class n) in counts
          do (loop repeat n
                   do (stack-task:submit-job q :work :resource-class class)))
    (let ((errors
            (%run-threads
             8
             (lambda (i)
               (let ((worker (format nil "w~d" i)))
                 (loop while (and (< (bt:with-lock-held (lock) done) total)
                                  (< (get-internal-real-time) deadline))
                       do (let ((job (stack-task:claim-job q worker)))
                            (if (null job)
                                (sleep 0.001)
                                (let ((class (stack-task:job-resource-class job)))
                                  (bt:with-lock-held (lock)
                                    (push (stack-task:job-id job) claimed)
                                    (let ((n (incf (cdr (assoc class active)))))
                                      (setf (cdr (assoc class max-active))
                                            (max n (cdr (assoc class max-active))))))
                                  (let ((u (stack-task:resource-class-usage q class)))
                                    (bt:with-lock-held (lock)
                                      (setf (cdr (assoc class max-usage))
                                            (max u (cdr (assoc class max-usage))))))
                                  (sleep 0.003)
                                  (bt:with-lock-held (lock)
                                    (decf (cdr (assoc class active))))
                                  (stack-task:complete-job
                                   q (stack-task:job-id job) worker
                                   :result i)
                                  (bt:with-lock-held (lock)
                                    (incf done)))))))))))
      (ok (null errors))
      (ok (= total (length claimed)))
      (ok (= total (length (remove-duplicates claimed :test #'equal))))
      (ok (= total (length (stack-task:list-jobs q :state :completed))))
      (loop for (class limit) in limits
            do (ok (<= (cdr (assoc class max-active)) limit))
               (ok (<= (cdr (assoc class max-usage)) limit))
               (ok (eql 0 (stack-task:resource-class-usage q class))))
      ;; The run really overlapped claims (else the bound proves nothing).
      (ok (>= (cdr (assoc :heavy max-usage)) 2)))))
