# task-backend-sql

SQL journal + worker leases for [`task-protocol`](https://github.com/egao1980/task-protocol) (`stack-task`). Depends on `task-protocol` and `sql-protocol`.

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

## Tests

Unit tests cover event encode/decode and the schema SQL strings. **Live SQLite is not run in wave-1** (`sql-backend-sqlite3` is not a test dependency). To exercise DDL locally:

```lisp
(asdf:load-system "sql-backend-sqlite3")
(asdf:load-system "task-backend-sql")
(sql-protocol:with-connection (c :driver :sqlite3 :database-name ":memory:")
  (task-backend-sql:ensure-journal-schema c))
```

## License

MIT — see [LICENSE](LICENSE).
