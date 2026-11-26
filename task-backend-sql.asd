(defsystem "task-backend-sql"
  :version "0.2.0"
  :description "sql-protocol journal, worker leases and job queue for task-protocol"
  :author "egao1980"
  :license "MIT"
  ;; bordeaux-threads: already in the graph through sql-protocol; the job
  ;; queue serializes its connection with a recursive lock.
  :depends-on ((:version "task-protocol" "0.3.0") "sql-protocol"
               "bordeaux-threads")
  :properties (:cl-repo
               (:ci (:with ("sql-backend-sqlite3")
                     :load-before-test ("sql-backend-sqlite3"))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "schema")
               (:file "backend")
               (:file "queue-schema")
               (:file "queue"))
  :in-order-to ((test-op (test-op "task-backend-sql/tests"))))

(defsystem "task-backend-sql/tests"
  :depends-on ("task-backend-sql" "sql-backend-sqlite3" "rove"
               "bordeaux-threads")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test")
               (:file "resume-test")
               (:file "conformance-support")
               (:file "conformance-queue-test")
               (:file "conformance-admission-test")
               (:file "conformance-progress-test")
               (:file "conformance-conditions-test")
               (:file "queue-sql-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
