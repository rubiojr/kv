package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadPoolIsReadOnly(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "kv.db"))
	require.NoError(t, db.Set("seed", []byte("value"), nil))
	_, err := db.RawReader().Exec("DELETE FROM key_values")
	assert.Error(t, err, "the reader pool must reject writes")
	value, err := db.Get("seed")
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)
}

func TestWritesQueueWithoutBlockingReads(t *testing.T) {
	db := openSQLite(t, filepath.Join(t.TempDir(), "kv.db"))
	require.NoError(t, db.Set("seed", []byte("value"), nil))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := db.Raw().Conn(ctx)
	require.NoError(t, err)
	defer conn.Close() //nolint:errcheck // Fallback if an assertion fails; explicit Close below is checked.
	before := db.Raw().Stats().WaitCount
	done := make(chan error, 1)
	go func() { done <- db.Set("written", []byte("new"), nil) }()
	var completed bool
	var writeError error
	require.Eventually(t, func() bool {
		select {
		case writeError = <-done:
			completed = true
			return true
		default:
			return db.Raw().Stats().WaitCount > before
		}
	}, 2*time.Second, time.Millisecond)
	assert.False(t, completed, "writes must queue for the reserved writer connection")
	// A queued writer must not consume or lock the reader pool.
	var value string
	require.NoError(t, db.RawReader().QueryRowContext(ctx, "SELECT value FROM key_values WHERE key='seed'").Scan(&value))
	assert.Equal(t, "value", value)
	require.NoError(t, conn.Close())
	if !completed {
		select {
		case writeError = <-done:
		case <-ctx.Done():
			t.Fatal("writer did not finish after its connection was released")
		}
	}
	require.NoError(t, writeError)
}

func TestClosePools(t *testing.T) {
	for _, dsn := range []string{filepath.Join(t.TempDir(), "kv.db"), ":memory:", ""} {
		db := openSQLite(t, dsn)
		writer, reader := db.Raw(), db.RawReader()
		require.NoError(t, db.Close())
		assert.Error(t, writer.Ping())
		assert.Error(t, reader.Ping())
		assert.NoError(t, db.Close(), "Close must be idempotent")
	}
}
