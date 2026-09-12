package sqlite

import (
	"context"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type resettingReader struct{ *pragmaConnection }

func (c *resettingReader) Begin() (driver.Tx, error) {
	return &resettingTransaction{conn: c.pragmaConnection}, nil
}

func (c *resettingReader) ResetSession(context.Context) error {
	c.queryOnly = "0"
	return nil
}

type resettingTransaction struct{ conn *pragmaConnection }

func (tx *resettingTransaction) Commit() error   { tx.conn.queryOnly = "0"; return nil }
func (tx *resettingTransaction) Rollback() error { tx.conn.queryOnly = "0"; return nil }

func TestReaderRestoresQueryOnly(t *testing.T) {
	for _, operation := range []string{"commit", "rollback", "reset"} {
		t.Run(operation, func(t *testing.T) {
			raw := &resettingReader{&pragmaConnection{mode: "wal", queryOnly: "1", supportsReads: true}}
			reader := &readerConnection{Conn: raw}
			if operation == "reset" {
				require.NoError(t, reader.ResetSession(t.Context()))
			} else {
				tx, err := reader.BeginTx(t.Context(), driver.TxOptions{})
				require.NoError(t, err)
				if operation == "commit" {
					require.NoError(t, tx.Commit())
				} else {
					require.NoError(t, tx.Rollback())
				}
			}
			assert.Equal(t, "1", raw.queryOnly)
			assert.True(t, reader.IsValid())
		})
	}
}

func TestReaderDiscardsFailedRestore(t *testing.T) {
	raw := &resettingReader{&pragmaConnection{mode: "wal", queryOnly: "1"}}
	reader := &readerConnection{Conn: raw}
	tx, err := reader.BeginTx(t.Context(), driver.TxOptions{})
	require.NoError(t, err)
	require.ErrorContains(t, tx.Commit(), "read-only")
	assert.False(t, reader.IsValid(), "a writable reader must not return to the pool")
}

func TestReaderResetFailureRejectsCheckout(t *testing.T) {
	raw := &resettingReader{&pragmaConnection{mode: "wal", queryOnly: "1"}}
	reader := &readerConnection{Conn: raw}
	require.ErrorIs(t, reader.ResetSession(t.Context()), driver.ErrBadConn)
	assert.False(t, reader.IsValid())
}
