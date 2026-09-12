package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingSetupConnection struct {
	driver.Conn
	err         error
	closeErr    error
	closed      bool
	failOnClose bool
}

func (c *failingSetupConnection) Prepare(string) (driver.Stmt, error) {
	if c.failOnClose {
		return &failingCloseStatement{err: c.err}, nil
	}
	return nil, c.err
}
func (c *failingSetupConnection) Close() error { c.closed = true; return c.closeErr }

type setupConnector struct {
	driver.Connector
	conn driver.Conn
}

type failingCloseStatement struct {
	driver.Stmt
	err error
}

func (s *failingCloseStatement) Exec([]driver.Value) (driver.Result, error) {
	return driver.RowsAffected(0), nil
}

func (s *failingCloseStatement) Close() error { return s.err }

func (c *setupConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }

func TestFailedConnectionSetupClosesConnection(t *testing.T) {
	for _, failOnClose := range []bool{false, true} {
		want := errors.New("setup failed")
		conn := &failingSetupConnection{err: want, failOnClose: failOnClose}
		connector := &configuredConnector{Connector: &setupConnector{conn: conn}}
		got, err := connector.Connect(t.Context())
		require.ErrorIs(t, err, want)
		assert.Nil(t, got)
		assert.True(t, conn.closed)
	}
}

func TestFailedConnectionSetupPreservesCloseError(t *testing.T) {
	setupErr := errors.New("setup failed")
	closeErr := errors.New("close failed")
	conn := &failingSetupConnection{err: setupErr, closeErr: closeErr}
	connector := &configuredConnector{Connector: &setupConnector{conn: conn}}
	got, err := connector.Connect(t.Context())
	require.ErrorIs(t, err, setupErr)
	assert.ErrorIs(t, err, closeErr)
	assert.Nil(t, got)
	assert.True(t, conn.closed)
}
