package sqlite_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rubiojr/kv/driver/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionContention(t *testing.T) {
	for _, database := range []struct {
		name string
		dsn  string
	}{
		{"memory", ":memory:"},
		{"temporary", ""},
		{"memory_uri", "file:kv-pool?mode=memory&cache=private"},
		{"file", filepath.Join(t.TempDir(), "kv.db")},
	} {
		t.Run(database.name, func(t *testing.T) {
			for _, operation := range []struct {
				name string
				run  func(*sqlite.Database) (any, error)
				want any
			}{
				{"get", func(db *sqlite.Database) (any, error) {
					return db.Get("seed")
				}, []byte("value")},
				{"mexists", func(db *sqlite.Database) (any, error) {
					return db.MExists("seed", "missing")
				}, []bool{true, false}},
				{"set", func(db *sqlite.Database) (any, error) {
					if err := db.Set("written", []byte("new value"), nil); err != nil {
						return nil, err
					}
					return db.Get("written")
				}, []byte("new value")},
			} {
				t.Run(operation.name, func(t *testing.T) {
					db := &sqlite.Database{}
					require.NoError(t, db.Init("key_values", database.dsn))
					t.Cleanup(func() { require.NoError(t, db.Close()) })
					require.NoError(t, db.Set("seed", []byte("value"), nil))

					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					// Hold the initialized connection while another goroutine uses KV.
					pool := db.RawReader()
					if operation.name == "set" {
						pool = db.Raw()
					}
					conn, err := pool.Conn(ctx)
					require.NoError(t, err)
					defer conn.Close() //nolint:errcheck // Fallback if an assertion fails; explicit Close below is checked.
					before := pool.Stats()
					type result struct {
						value any
						err   error
					}
					done := make(chan result, 1)
					go func() {
						value, err := operation.run(db)
						done <- result{value, err}
					}()

					// Wait for pool expansion or a queued operation, rather than relying
					// on a sleep to ensure the worker contends for the connection.
					require.Eventually(t, func() bool {
						stats := pool.Stats()
						return stats.OpenConnections > before.OpenConnections || stats.WaitCount > before.WaitCount
					}, 5*time.Second, time.Millisecond)
					require.NoError(t, conn.Close())

					select {
					case got := <-done:
						require.NoError(t, got.err)
						assert.Equal(t, operation.want, got.value)
					case <-ctx.Done():
						t.Fatal("KV operation did not finish after releasing the connection")
					}
				})
			}
		})
	}
}
