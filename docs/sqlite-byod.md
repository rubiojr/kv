# SQLite BYOD

KV uses modernc by default. To bring your own SQLite driver, import or register it
with `database/sql` and pass its registered name.

## Choosing a driver

```Go
import (
    "time"

    _ "github.com/mattn/go-sqlite3"
    "github.com/rubiojr/kv"
)

db, err := kv.New("sqlite", "my.db",
    kv.WithSQLiteDriver("sqlite3"),
    kv.WithSQLiteBusyTimeout(2*time.Second),
)
if err != nil {
    panic(err)
}
defer db.Close()
```

The ncruces driver (`github.com/ncruces/go-sqlite3/driver`) also registers as
`sqlite3`. Select one of these imports when using that registered name.

The DSN is passed to the supplied driver unchanged. Omitting `WithSQLiteDriver`
or passing an empty name uses modernc. Unknown driver names return an error.
The adapter supports both `driver.Driver` and `driver.DriverContext` and configures
connections with standard SQLite SQL, without vendor-specific DSN options.

## Connection pools

File-backed databases require **WAL mode** and use two pools:

- `Raw()` is the writer pool, limited to one connection. `database/sql` queues
  competing writes; there is no write mutex.
- `RawReader()` is a read-only pool with up to eight connections. Reads can run
  concurrently with the writer. Its size can be tuned through `RawReader()`.

The writer connection limit should stay at one. SQLite coordinates writes from
other handles and processes using its own locks and the busy timeout.

Connections get a 5-second busy timeout by default. Use `WithSQLiteBusyTimeout`
to change it; zero disables waiting. Settings are applied after the supplied
driver opens each connection, using standard SQLite PRAGMAs.
Other settings, including `synchronous`, are preserved (modernc defaults to `FULL`).

## WAL requirements

**BYOD drivers must support WAL for file-backed stores.** Every writer connection
enables and verifies WAL. Every reader connection verifies existing WAL mode and
enables and verifies `query_only`. Both pools are validated during initialization;
unsupported modes return an error rather than falling back to rollback journaling.
Reader mode is restored after transaction cleanup and driver session resets.
WAL creates `-wal` and `-shm` sidecar files and requires a compatible local filesystem.

## Memory databases

Private `:memory:` databases, private memory URIs, and empty-DSN temporary databases
share one connection for reads and writes so their state survives between calls.
Their reader pool is therefore also writable. Explicitly shared memory URIs can
use separate pools. Memory and temporary databases are exempt from the WAL
requirement because SQLite cannot use WAL for them.

## Closing the store

Use `db.Close()` to close the entire store. `Raw().Close()` closes only the writer
pool when the pools are separate. Release rows, reserved connections, and
transactions before closing the store or requesting an occupied connection.
For private memory/temp databases, changing pool limits or connection lifetime can
discard or split database state.
