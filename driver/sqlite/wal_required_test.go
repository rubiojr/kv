package sqlite

import (
	"database/sql/driver"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Simulate a supplied driver that can refuse or ignore a requested PRAGMA.
type pragmaConnection struct {
	driver.Conn
	mode          string
	supportsWAL   bool
	supportsReads bool
	queryOnly     string
	queries       []string
	closed        bool
}

func (c *pragmaConnection) Prepare(query string) (driver.Stmt, error) {
	c.queries = append(c.queries, query)
	return &pragmaStatement{conn: c, query: query}, nil
}

func (c *pragmaConnection) Close() error { c.closed = true; return nil }

type pragmaStatement struct {
	driver.Stmt
	conn  *pragmaConnection
	query string
}

func (s *pragmaStatement) Close() error { return nil }

func (s *pragmaStatement) Exec([]driver.Value) (driver.Result, error) {
	if s.query == "PRAGMA query_only=ON" && s.conn.supportsReads {
		s.conn.queryOnly = "1"
	}
	return driver.RowsAffected(0), nil
}

func (s *pragmaStatement) Query([]driver.Value) (driver.Rows, error) {
	value := "file.db"
	switch s.query {
	case "PRAGMA journal_mode=WAL":
		if s.conn.supportsWAL {
			s.conn.mode = "wal"
		}
		value = s.conn.mode
	case "PRAGMA journal_mode":
		value = s.conn.mode
	case "PRAGMA query_only":
		value = s.conn.queryOnly
	}
	return &pragmaRows{value: value}, nil
}

type pragmaRows struct {
	value string
	done  bool
}

func (r *pragmaRows) Columns() []string { return []string{"value"} }
func (r *pragmaRows) Close() error      { return nil }
func (r *pragmaRows) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	values[0] = r.value
	return nil
}

func TestSuppliedConnectionRequiresWAL(t *testing.T) {
	for _, tc := range []struct {
		name, mode, wantError string
		readOnly, wal, reads  bool
	}{
		{"writer_refuses_wal", "delete", "WAL", false, false, true},
		{"reader_requires_existing_wal", "delete", "WAL", true, true, true},
		{"reader_refuses_query_only", "wal", "read-only", true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &pragmaConnection{mode: tc.mode, supportsWAL: tc.wal, supportsReads: tc.reads, queryOnly: "0"}
			connector := &configuredConnector{Connector: &setupConnector{conn: conn}, readOnly: tc.readOnly}
			got, err := connector.Connect(t.Context())
			require.ErrorContains(t, err, tc.wantError)
			assert.Nil(t, got)
			assert.True(t, conn.closed)
			if tc.readOnly {
				assert.NotContains(t, conn.queries, "PRAGMA journal_mode=WAL", "readers must not change journal mode")
			}
		})
	}
}
