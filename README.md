# task-backend-sql

SQL journal, worker leases and job queue for [`task-protocol`](https://github.com/egao1980/task-protocol) (`stack-task`). Depends on `task-protocol` (0.3.0 or later), `sql-protocol` and `bordeaux-threads` (already in the graph through `sql-protocol`).

```lisp
(asdf:load-system "task-backend-sql")
;; plus a driver: sql-backend-sqlite3 (personal) or sql-backend-postgres (corporate)

(sql-protocol:with-connection (c :driver :sqlite3 :database-name ":memory:")
  (let* ((journal (task-backend-sql:make-sql-task-journal :connection c))
         (task (stack-task:make-durable-task :id "demo")))
    (stack-task:with-durable-task (task journal)
      (stack-task:with-durable-step ("boot") t))))
```

| Piece | Notes |
|-------|--------|
| `sql-task-journal` | Implements `append-event` / `replay-journal` on table `(task_id, seq, type, payload)` |
| `+journal-ddl+` / `+lease-ddl+` | Wave-1 schema source of truth |
| `ensure-journal-schema` | `CREATE TABLE IF NOT EXISTS` via `sql-protocol:execute` |
| `migrations/0001-task-journal.lisp` | sql-migrate-shaped revision metadata (`make-journal-revision`) — register when `sql-orm` + `sql-migrate` are loaded. sql-migrate 0.1.0 is in-memory `script-directory`, not a file runner. |
| `claim-task` / `heartbeat` / `release-task` | SQLite-friendly expired-lease claim (no `SKIP LOCKED`). Postgres `SELECT … FOR UPDATE SKIP LOCKED` is a TODO. |

## 0.2.0 (contract): job queue

`sql-job-queue` implements the task-protocol 0.3.0 job-queue generics
(`submit-job`, `claim-job`, `define-resource-class`, `report-job-progress`,
`subscribe-job-events`, …) on `sql-protocol`, with the observable semantics
of task-protocol's in-memory queue: the spec is the header of
task-protocol `src/jobs.lisp`, and its queue tests run here as a
conformance suite. Issue [#3](https://github.com/egao1980/task-backend-sql/issues/3).

```lisp
(sql-protocol:with-connection (c :driver :sqlite3 :database-name "jobs.sqlite")
  (let* ((q (task-backend-sql:make-sql-job-queue
             :connection c :resource-classes '((:heavy 2))))
         (id (stack-task:submit-job q :render :payload '(:input "a.dat")
                                    :resource-class :heavy)))
    (let ((job (stack-task:claim-job q "worker-1")))
      (when job
        (stack-task:report-job-progress q id "worker-1" :fraction 1/2)
        (stack-task:complete-job q id "worker-1" :result '(:ok t))))))
```

| Piece | Notes |
|-------|--------|
| `make-sql-job-queue` | `:connection` (required) `:journal` (default: a `sql-task-journal` on the same connection) `:resource-classes` `((name limit) …)` `:ensure-schema` (t) `:dialect` (`:sqlite3`) `:busy-timeout` (ms, 5000) |
| `task_job` | one row per job. `submit_seq` is the FIFO key, set once at submit, so lease expiry keeps a job's place. Values (`kind`, `payload`, `result`, `failure`) are `encode-value` text |
| `task_resource_class` | name, `job_limit`, definition `position`. Shared by every connection. Usage is `COUNT(*)` of running jobs, not a stored counter |
| `task_journal` | (revision 0001) carries every job event: transitions and progress, `task_id` = job id, readable from a seq with `job-events :after-seq` |
| `+job-ddl+` `+resource-class-ddl+` `+job-index-ddl+` | revision 0002 source of truth; `ensure-queue-schema` applies 0001 + 0002 |
| `migrations/0002-job-queue.lisp` | `queue-revision-plist`, down revision 0001 |

- **Atomic claim.** Each changing operation is one transaction, and on
  SQLite it starts with `BEGIN IMMEDIATE`. So `claim-job` runs lease
  expiry, the candidate scan, the class usage count, the limit check and
  the update under the write lock. Usage never exceeds a limit across
  connections or processes, and a state change commits together with its
  `job-transition` event. An unwound operation rolls back.
- **Leases.** Expired when `lease_until < now` (strict), like the in-memory
  backend and `lease-claimable-p`. `claim-job` expires leases first in the
  same transaction.
- **Subscriptions.** Live delivery works within one process, per queue
  object. It runs after commit, in seq order, with gapless replay. Another
  process, or another queue object, polls `job-events :after-seq` with the
  last seq it saw; a new subscription replays everything committed.
- **Threads.** One queue holds one connection behind a recursive lock and
  can be shared by threads. More connections coordinate through the
  database.
- **Postgres: TODO.** The claim wants `SELECT … FOR UPDATE SKIP LOCKED` plus
  a lock on the class row. Not implemented: `:dialect :postgres` signals
  `task-error`, and the test `postgres-claim-skip-locked` is skipped.
- **Hash tables** have no column text: `encode-value` signals
  `task-serialization-error` with `use-value`.

The job state machine stays task-protocol's one table
(`job-transition-allowed-p`); admission is a SQL predicate over the job and
class rows, not new rule code.

| File | Status |
|------|--------|
| `src/queue-schema.lisp` (DDL, revision 0002, `ensure-queue-schema`, `encode-value` / `decode-value`) | given |
| `src/queue.lisp` (`sql-job-queue`, `make-sql-job-queue`) | given |
| `src/queue.lisp` (every queue method) | implemented |
| `tests/conformance-*.lisp`, `tests/queue-sql-test.lisp` | contract |

## Tests

`cl-repo test --system task-backend-sql`. `sql-backend-sqlite3` is a test dependency: the queue tests run on live SQLite (in-memory databases and temp files).

| File | Covers |
|------|--------|
| `tests/backend-test.lisp` | journal and lease DDL strings, event codec (0.1.1) |
| `tests/resume-test.lisp` | cross-process kill-and-resume on a shared SQLite journal (0.1.1) |
| `tests/conformance-*.lisp` | the task-protocol 0.3.0 job-queue suite (`tests/job-*-test.lisp`) against `sql-job-queue` |
| `tests/queue-sql-test.lisp` | revision 0002 on an empty database, column codec, concurrent connections under class limits, reopen with lease expiry and re-claim, polling from another connection, delivery after commit, rollback |

## License

MIT — see [LICENSE](LICENSE).
