package kv_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rubiojr/kv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var driverSequence atomic.Uint64

// opaqueDriver accepts its own DSN syntax and initializes connection state.
// The KV adapter must leave the DSN intact and preserve unrelated settings.
type opaqueDriver struct {
	base   driver.Driver
	dsn    string
	opens  atomic.Int32
	legacy bool
}

const opaqueDSN = "application://store?token=a%2Fb&custom=unchanged"

func (d *opaqueDriver) Open(dsn string) (driver.Conn, error) {
	if dsn != opaqueDSN {
		return nil, fmt.Errorf("driver received a rewritten DSN: %q", dsn)
	}
	conn, err := d.base.Open(d.dsn)
	if err != nil {
		return nil, err
	}
	stmt, err := conn.Prepare("PRAGMA synchronous=NORMAL")
	if err == nil {
		_, err = stmt.(driver.StmtExecContext).ExecContext(context.Background(), nil)
		err = errors.Join(err, stmt.Close())
	}
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	d.opens.Add(1)
	if d.legacy {
		return &legacyConnection{Conn: conn}, nil
	}
	return conn, nil
}

// Hide optional context interfaces to exercise the standard legacy fallback.
type legacyConnection struct{ driver.Conn }

func (c *legacyConnection) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.Conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &legacyStatement{Stmt: stmt}, nil
}

type legacyStatement struct{ driver.Stmt }

type contextDriver struct {
	*opaqueDriver
	connectors atomic.Int32
	closes     atomic.Int32
}

func (d *contextDriver) Open(string) (driver.Conn, error) {
	return nil, fmt.Errorf("DriverContext must be opened through its connector")
}

func (d *contextDriver) OpenConnector(dsn string) (driver.Connector, error) {
	d.connectors.Add(1)
	return &opaqueConnector{owner: d, dsn: dsn}, nil
}

type opaqueConnector struct {
	owner *contextDriver
	dsn   string
}

func (c *opaqueConnector) Driver() driver.Driver { return c.owner }

func (c *opaqueConnector) Close() error {
	c.owner.closes.Add(1)
	return nil
}

func (c *opaqueConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.owner.opaqueDriver.Open(c.dsn)
}

func TestSQLiteDriver(t *testing.T) {
	probe, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	base := probe.Driver()
	require.NoError(t, probe.Close())
	for _, useConnector := range []bool{false, true} {
		t.Run(fmt.Sprintf("connector_%t", useConnector), func(t *testing.T) {
			custom := &opaqueDriver{base: base, dsn: filepath.Join(t.TempDir(), "kv.db"), legacy: !useConnector}
			var supplied driver.Driver = custom
			if useConnector {
				supplied = &contextDriver{opaqueDriver: custom}
			}
			name := fmt.Sprintf("kv-byod-%d", driverSequence.Add(1))
			sql.Register(name, supplied)
			db, err := kv.New("sqlite", opaqueDSN, kv.WithSQLiteDriver(name), kv.WithSQLiteBusyTimeout(123*time.Millisecond))
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, db.Close())
				if contextual, ok := supplied.(*contextDriver); ok {
					assert.Equal(t, contextual.connectors.Load(), contextual.closes.Load(), "close every connector")
				}
			})
			value := []byte{0, 255, 1, 0}
			require.NoError(t, db.Set("key", value, nil))
			got, err := db.Get("key")
			require.NoError(t, err)
			assert.Equal(t, value, got)

			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			for _, pool := range []*sql.DB{db.Raw(), db.RawReader(), db.RawReader()} {
				conn, err := pool.Conn(ctx)
				require.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, conn.Close()) })
				var mode string
				var busy, synchronous int
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode))
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busy))
				require.NoError(t, conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous))
				assert.Equal(t, "wal", mode)
				assert.Equal(t, 123, busy)
				assert.Equal(t, 1, synchronous, "preserve the supplied driver's durability setting")
			}
			assert.GreaterOrEqual(t, custom.opens.Load(), int32(2))
		})
	}
}

func TestSQLiteInvalidOptions(t *testing.T) {
	for _, tc := range []struct {
		name, backend, message string
		option                 kv.Option
	}{
		{"unknown_driver", "sqlite", "unknown driver", kv.WithSQLiteDriver("kv-unregistered")},
		{"negative_timeout", "sqlite", "busy timeout", kv.WithSQLiteBusyTimeout(-time.Second)},
		{"wrong_backend", "mysql", "SQLite options", kv.WithSQLiteDriver("sqlite")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "kv.db")
			db, err := kv.New(tc.backend, path, tc.option)
			if db != nil && db.Raw() != nil {
				t.Cleanup(func() { require.NoError(t, db.Close()) })
			}
			require.ErrorContains(t, err, tc.message)
			assert.NoFileExists(t, path)
		})
	}
}
