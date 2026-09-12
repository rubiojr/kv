package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/rubiojr/kv/driver/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openSQLite(t *testing.T, dsn string) *sqlite.Database {
	t.Helper()
	db := &sqlite.Database{}
	require.NoError(t, db.Init("key_values", dsn))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestWALConnections(t *testing.T) {
	custom := 123 * time.Millisecond
	disabled := time.Duration(0)
	for _, tc := range []struct {
		name    string
		timeout *time.Duration
		busy    int
	}{
		{"defaults", nil, 5000},
		{"custom", &custom, 123},
		{"disabled", &disabled, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &sqlite.Database{BusyTimeout: tc.timeout}
			require.NoError(t, db.Init("key_values", filepath.Join(t.TempDir(), "kv.db")))
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			// Force a second physical connection to verify connection-local settings.
			db.RawReader().SetMaxOpenConns(2)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for i, pool := range []*sql.DB{db.Raw(), db.RawReader(), db.RawReader()} {
				conn, err := pool.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				var journal string
				var busy, synchronous int
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal))
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy))
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous))
				assert.Equal(t, "wal", journal)
				assert.Equal(t, tc.busy, busy)
				assert.Equal(t, 2, synchronous)
				var queryOnly int
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA query_only").Scan(&queryOnly))
				if i == 0 {
					assert.Zero(t, queryOnly)
				} else {
					assert.Equal(t, 1, queryOnly)
				}
			}
		})
	}
}

func TestWALWriteDuringRead(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "kv.db"))
	require.NoError(t, db.Set("key", []byte("old"), nil))
	tx, err := db.RawReader().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	require.NoError(t, err)
	defer tx.Rollback()
	var snapshot string
	require.NoError(t, tx.QueryRow("SELECT value FROM key_values WHERE key='key'").Scan(&snapshot))

	done := make(chan error, 1)
	go func() { done <- db.Set("key", []byte("new"), nil) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("write blocked behind an active reader")
	}
	require.NoError(t, tx.QueryRow("SELECT value FROM key_values WHERE key='key'").Scan(&snapshot))
	assert.Equal(t, "old", snapshot, "the reader must retain its original snapshot")
	require.NoError(t, tx.Commit())
	value, err := db.Get("key")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), value)
}

func TestWALReadDuringWrite(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "kv.db"))
	require.NoError(t, db.Set("key", []byte("old"), nil))
	tx, err := db.Raw().BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.Exec("UPDATE key_values SET value='uncommitted' WHERE key='key'")
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		value, err := db.Get("key")
		if err == nil && string(value) != "old" {
			err = fmt.Errorf("read uncommitted value %q", value)
		}
		done <- err
	}()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("read blocked behind an active writer")
	}
}

func TestWALConcurrentWrites(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "kv.db")
	db := openSQLite(t, dsn)
	other := openSQLite(t, dsn)
	start := make(chan struct{})
	done := make(chan error, 16)
	for i := range 16 {
		go func() {
			<-start
			writer := db
			if i%2 == 0 {
				writer = other
			}
			key := fmt.Sprintf("key-%d", i)
			if err := writer.Set(key, []byte("value"), nil); err != nil {
				done <- err
				return
			}
			done <- writer.Del(key)
		}()
	}
	close(start)
	for range 16 {
		require.NoError(t, <-done)
	}
	var count int
	require.NoError(t, db.Raw().QueryRow("SELECT count(*) FROM key_values").Scan(&count))
	assert.Zero(t, count)
}

func TestSharedMemoryConnections(t *testing.T) {
	db := openSQLite(t, "file:kv-shared?mode=memory&cache=shared")
	require.NoError(t, db.Set("key", []byte("value"), nil))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for range 2 {
		conn, err := db.RawReader().Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var value string
		require.NoError(t, conn.QueryRowContext(ctx, "SELECT value FROM key_values WHERE key='key'").Scan(&value))
		assert.Equal(t, "value", value)
	}
}
