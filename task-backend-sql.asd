(defsystem "task-backend-sql"
  :version "0.1.0"
  :description "sql-protocol journal + worker leases for task-protocol"
  :author "egao1980"
  :license "MIT"
  :depends-on ("task-protocol" "sql-protocol")
  :properties (:cl-repo
               (:ci (:with ("sql-backend-sqlite3")
                     :load-before-test ("sql-backend-sqlite3"))))
  :serial t
  :pathname "src"
  :components ((:file "package")
               (:file "schema")
               (:file "backend"))
  :in-order-to ((test-op (test-op "task-backend-sql/tests"))))

(defsystem "task-backend-sql/tests"
  :depends-on ("task-backend-sql" "sql-backend-sqlite3" "rove")
  :pathname "tests"
  :serial t
  :components ((:file "package")
               (:file "backend-test")
               (:file "resume-test"))
  :perform (test-op (o c)
             (unless (symbol-call :rove :run c)
               (error "tests failed for ~A" (component-name c)))))
