(in-package #:task-backend-sql/tests)

;;; A6c: true cross-process kill-and-resume against a shared SQLite journal.
;;; Parent writes a child script, launch-program SBCL runs a 3-step durable
;;; task, child sb-ext:exit :abort t after step 2's journal append. Parent
;;; replays: steps 1–2 from the journal (side-effect files once, keyed by
;;; idempotency key); step 3 executes fresh.

(defparameter +resume-task-id+ "resume-cross-proc")

(defun %effect-path (fx-dir name)
  (merge-pathnames (format nil "~a.txt" name)
                   (uiop:ensure-directory-pathname fx-dir)))

(defun write-effect (fx-dir name)
  "Append NAME to FX-DIR/NAME.txt (one line per execution)."
  (let ((path (%effect-path fx-dir name)))
    (ensure-directories-exist path)
    (with-open-file (out path :direction :output
                         :if-exists :append
                         :if-does-not-exist :create)
      (write-line name out)
      (finish-output out))
    path))

(defun effect-line-count (fx-dir name)
  (let ((path (%effect-path fx-dir name)))
    (if (probe-file path)
        (with-open-file (in path)
          (loop for line = (read-line in nil nil)
                while line
                count t))
        0)))

(defun %sbcl ()
  (or (let ((env (uiop:getenv "SBCL")))
        (and env (plusp (length env)) env))
      #+sbcl (ignore-errors
               (namestring (truename sb-ext:*runtime-pathname*)))
      (let ((which (ignore-errors
                     (string-trim '(#\Space #\Newline #\Return #\Tab)
                                  (uiop:run-program '("which" "sbcl")
                                                    :output :string
                                                    :ignore-error-status t)))))
        (and which (plusp (length which)) which))
      "sbcl"))

(defun %system-dir (name)
  (ignore-errors
    (namestring (truename (asdf:system-source-directory name)))))

(defun %sibling-task-protocol-dir ()
  (let ((backend (%system-dir "task-backend-sql")))
    (when backend
      (let ((candidate (merge-pathnames
                        "../task-protocol/"
                        (uiop:ensure-directory-pathname backend))))
        (when (probe-file (merge-pathnames "task-protocol.asd" candidate))
          (namestring (truename candidate)))))))

(defun %probe-dir (path)
  (when (and path (plusp (length (namestring path))) (probe-file path))
    (namestring (truename (uiop:ensure-directory-pathname path)))))

(defun %systems-root-dir ()
  "OCI / setup-client dest so the child sees dbd-sqlite3 and friends."
  (or (ignore-errors
        (let* ((pkg (find-package '#:cl-repository-client/installer))
               (sym (and pkg (find-symbol "SYSTEMS-ROOT" pkg))))
          (when (and sym (fboundp sym))
            (%probe-dir (funcall sym)))))
      (%probe-dir (uiop:getenv "CL_REPOSITORY_DEST"))
      (%probe-dir (merge-pathnames ".local/share/cl-repository/systems/"
                                   (user-homedir-pathname)))))

(defun %child-registry-dirs ()
  (let ((dirs '()))
    (flet ((add (p)
             (let ((dir (%probe-dir p)))
               (when dir
                 (pushnew dir dirs :test #'string-equal)))))
      (add (%system-dir "task-backend-sql"))
      (add (or (%system-dir "task-protocol") (%sibling-task-protocol-dir)))
      (dolist (name '("sql-protocol" "sql-backend-sqlite3" "dbd-sqlite3"
                      "dbi" "cl-dbi" "rove"))
        (add (%system-dir name))))
    (nreverse dirs)))

(defun %write-child-script (script &key db fx-dir marker task-id
                                     dirs trees)
  (with-open-file (out script :direction :output :if-exists :supersede)
    (format out ";;;; Generated A6c kill-and-resume child. Do not edit.~%")
    (format out "(require :asdf)~%")
    (format out "#+sbcl (sb-ext:disable-debugger)~%")
    (format out "(setf *debugger-hook*~%")
    (format out "      (lambda (c h)~%")
    (format out "        (declare (ignore h))~%")
    (format out "        (format *error-output* \"~~&CHILD-ERROR: ~~A~~%\" c)~%")
    (format out "        #+sbcl (sb-ext:exit :code 1)~%")
    (format out "        #-sbcl (quit)))~%")
    (format out "(asdf:initialize-source-registry~%")
    (format out " '(:source-registry~%")
    (dolist (dir dirs)
      (format out "   (:directory ~s)~%" (pathname dir)))
    (dolist (tree trees)
      (format out "   (:tree ~s)~%" (pathname tree)))
    (format out "   :inherit-configuration))~%")
    (format out "(asdf:load-system \"sql-backend-sqlite3\")~%")
    (format out "(asdf:load-system \"task-backend-sql\")~%")
    (format out "(let* ((db ~s)~%" (namestring db))
    (format out "       (fx #p~s)~%" (namestring (uiop:ensure-directory-pathname fx-dir)))
    (format out "       (marker #p~s)~%" (namestring marker))
    (format out "       (task-id ~s))~%" task-id)
    (format out "  (flet ((write-effect (name)~%")
    (format out "           (let ((path (merge-pathnames (format nil \"~~a.txt\" name) fx)))~%")
    (format out "             (ensure-directories-exist path)~%")
    (format out "             (with-open-file (o path :direction :output~%")
    (format out "                              :if-exists :append~%")
    (format out "                              :if-does-not-exist :create)~%")
    (format out "               (write-line name o)~%")
    (format out "               (finish-output o)))))~%")
    (format out "    (sql-protocol:with-connection (c :driver :sqlite3 :database-name db)~%")
    (format out "      (let* ((journal (task-backend-sql:make-sql-task-journal :connection c))~%")
    (format out "             (task (task-protocol:make-durable-task :id task-id)))~%")
    (format out "        (task-protocol:with-durable-task (task journal)~%")
    (format out "          (task-protocol:with-durable-step (\"step-1\" :idempotency-key \"k-1\")~%")
    (format out "            (write-effect \"step-1\")~%")
    (format out "            :step-1)~%")
    (format out "          (task-protocol:with-durable-step (\"step-2\" :idempotency-key \"k-2\")~%")
    (format out "            (write-effect \"step-2\")~%")
    (format out "            :step-2)~%")
    (format out "          (sql-protocol:execute c \"SELECT COUNT(*) FROM task_journal\")~%")
    (format out "          (with-open-file (m marker :direction :output :if-exists :supersede)~%")
    (format out "            (write-line \"killed-after-step-2\" m)~%")
    (format out "            (finish-output m))~%")
    (format out "          #+sbcl (sb-ext:exit :abort t)~%")
    (format out "          #-sbcl (uiop:quit 0)~%")
    (format out "          (task-protocol:with-durable-step (\"step-3\" :idempotency-key \"k-3\")~%")
    (format out "            (write-effect \"step-3\")~%")
    (format out "            :step-3))))))~%")))

(defun %wait-child (proc &key (timeout 90))
  (loop with start = (get-internal-real-time)
        for elapsed = (/ (- (get-internal-real-time) start)
                         internal-time-units-per-second)
        do (unless (uiop:process-alive-p proc)
             (return (uiop:wait-process proc)))
           (when (> elapsed timeout)
             (uiop:terminate-process proc :urgent t)
             (error "child SBCL timed out after ~a seconds" timeout))
           (sleep 0.2)))

(defun %slurp (path)
  (if (probe-file path)
      (uiop:read-file-string path)
      ""))

(defun %journal-steps (journal task)
  (remove-if-not (lambda (e) (typep e 'task-protocol:step-completed))
                 (task-protocol:journal-events journal task)))

(defun %run-three-steps (fx-dir &key (fresh-counters (vector 0 0 0)))
  "Run the 3-step durable body. FRESH-COUNTERS tracks live (not replayed) execs."
  (values
   (task-protocol:with-durable-step ("step-1" :idempotency-key "k-1")
     (incf (aref fresh-counters 0))
     (write-effect fx-dir "step-1")
     :must-not-reexec-1)
   (task-protocol:with-durable-step ("step-2" :idempotency-key "k-2")
     (incf (aref fresh-counters 1))
     (write-effect fx-dir "step-2")
     :must-not-reexec-2)
   (task-protocol:with-durable-step ("step-3" :idempotency-key "k-3")
     (incf (aref fresh-counters 2))
     (write-effect fx-dir "step-3")
     :step-3)
   fresh-counters))

(deftest sql-journal-kill-and-resume
  #-sbcl
  (skip "cross-process resume requires SBCL (sb-ext:exit :abort t)")
  #+sbcl
  (let* ((root (ensure-directories-exist
                (merge-pathnames
                 (format nil "task-resume-~d-~d/"
                         (get-universal-time)
                         (random 1000000))
                 (uiop:temporary-directory))))
         (db (merge-pathnames "journal.sqlite" root))
         (script (merge-pathnames "child.lisp" root))
         (log (merge-pathnames "child.log" root))
         (fx (ensure-directories-exist (merge-pathnames "fx/" root)))
         (marker (merge-pathnames "killed-after-step-2" root))
         (dirs (%child-registry-dirs))
         (trees (remove nil (list (%systems-root-dir)
                                  (%probe-dir (uiop:getenv "CL_REPOSITORY_DEST"))))))
    (unwind-protect
         (progn
           (%write-child-script script
                                :db db :fx-dir fx :marker marker
                                :task-id +resume-task-id+
                                :dirs dirs
                                :trees trees)
           (let* ((argv (list (%sbcl) "--noinform" "--non-interactive"
                              "--disable-debugger"
                              "--load" (uiop:native-namestring script)))
                  (proc (uiop:launch-program
                         argv
                         :output (uiop:native-namestring log)
                         :error-output :output
                         :if-output-exists :supersede))
                  (code (%wait-child proc)))
             (unless (probe-file marker)
               (error "child did not reach step-2 abort (exit ~a)~%~a"
                      code (%slurp log)))
             (ok (probe-file marker)
                 "child aborted after step 2 journal append")
             (ok (= 1 (effect-line-count fx "step-1"))
                 "child wrote step-1 side-effect once")
             (ok (= 1 (effect-line-count fx "step-2"))
                 "child wrote step-2 side-effect once")
             (ok (zerop (effect-line-count fx "step-3"))
                 "child never ran step 3"))
           (sql-protocol:with-connection (c :driver :sqlite3
                                            :database-name (namestring db))
             (let* ((journal (task-backend-sql:make-sql-task-journal
                              :connection c))
                    (task (task-protocol:make-durable-task
                           :id +resume-task-id+))
                    (before (%journal-steps journal task)))
               (ok (= 2 (length before))
                   "shared journal has exactly two completed steps")
               (ok (equal '("step-1" "step-2")
                          (mapcar #'task-protocol:step-name before)))
               (ok (equal '("k-1" "k-2")
                          (mapcar #'task-protocol:step-idempotency-key before)))
               (ok (equal '(:step-1 :step-2)
                          (mapcar #'task-protocol:step-result before)))
               (let ((fresh (vector 0 0 0)))
                 (task-protocol:with-durable-task (task journal)
                   (multiple-value-bind (r1 r2 r3 counters)
                       (%run-three-steps fx :fresh-counters fresh)
                     (ok (eq :step-1 r1)
                         "step 1 result comes from journal replay")
                     (ok (eq :step-2 r2)
                         "step 2 result comes from journal replay")
                     (ok (eq :step-3 r3)
                         "step 3 executes fresh")
                     (ok (zerop (aref counters 0))
                         "step 1 body did not re-execute")
                     (ok (zerop (aref counters 1))
                         "step 2 body did not re-execute")
                     (ok (= 1 (aref counters 2))
                         "step 3 body ran once")))
                 (ok (= 1 (effect-line-count fx "step-1"))
                     "step-1 side-effect file written exactly once")
                 (ok (= 1 (effect-line-count fx "step-2"))
                     "step-2 side-effect file written exactly once")
                 (ok (= 1 (effect-line-count fx "step-3"))
                     "step-3 side-effect file written on resume")
                 (let ((after (%journal-steps journal task)))
                   (ok (= 3 (length after))
                       "resume appended only the fresh step 3")
                   (ok (equal "k-3" (task-protocol:step-idempotency-key
                                     (third after))))
                   (ok (eq :step-3 (task-protocol:step-result
                                    (third after)))))))))
      (ignore-errors
        (uiop:delete-directory-tree
         root
         :validate (lambda (p) (search "task-resume-" (namestring p)))
         :if-does-not-exist :ignore)))))
