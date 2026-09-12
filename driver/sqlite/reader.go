package sqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
)

// readerConnection preserves its role when a supplied driver resets query_only
// after a transaction or during pool reuse. Optional interfaces are forwarded.
type readerConnection struct {
	driver.Conn
	bad bool
}

func (c *readerConnection) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return prepareConnection(ctx, c.Conn, query)
}

func (c *readerConnection) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if execer, ok := c.Conn.(driver.ExecerContext); ok {
		return execer.ExecContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *readerConnection) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if queryer, ok := c.Conn.(driver.QueryerContext); ok {
		return queryer.QueryContext(ctx, query, args)
	}
	return nil, driver.ErrSkip
}

func (c *readerConnection) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := c.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

func (c *readerConnection) Ping(ctx context.Context) error {
	if pinger, ok := c.Conn.(driver.Pinger); ok {
		return pinger.Ping(ctx)
	}
	return ctx.Err()
}

func (c *readerConnection) ResetSession(ctx context.Context) error {
	if resetter, ok := c.Conn.(driver.SessionResetter); ok {
		if err := resetter.ResetSession(ctx); err != nil {
			c.bad = true
			return errors.Join(driver.ErrBadConn, err)
		}
		if err := c.restoreReadOnly(ctx); err != nil {
			// database/sql discards failed checkouts only for ErrBadConn.
			return errors.Join(driver.ErrBadConn, err)
		}
	}
	return nil
}

func (c *readerConnection) IsValid() bool {
	if c.bad {
		return false
	}
	if validator, ok := c.Conn.(driver.Validator); ok {
		return validator.IsValid()
	}
	return true
}

func (c *readerConnection) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *readerConnection) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var tx driver.Tx
	var err error
	opts.ReadOnly = true
	if beginner, ok := c.Conn.(driver.ConnBeginTx); ok {
		tx, err = beginner.BeginTx(ctx, opts)
	} else {
		if opts.Isolation != 0 {
			return nil, fmt.Errorf("sqlite reader: driver does not support the requested isolation level")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		//lint:ignore SA1019 Required fallback; query_only already enforces read-only access.
		tx, err = c.Conn.Begin()
	}
	if err != nil {
		return nil, errors.Join(err, c.restoreReadOnly(context.Background()))
	}
	return &readerTransaction{Tx: tx, conn: c}, nil
}

func (c *readerConnection) restoreReadOnly(ctx context.Context) error {
	err := configureReadOnly(ctx, c.Conn)
	if err != nil {
		c.bad = true // database/sql must discard a connection whose role cannot be restored.
	}
	return err
}

type readerTransaction struct {
	driver.Tx
	conn *readerConnection
}

func (tx *readerTransaction) Commit() error {
	err := tx.Tx.Commit()
	return errors.Join(err, tx.conn.restoreReadOnly(context.Background()))
}

func (tx *readerTransaction) Rollback() error {
	err := tx.Tx.Rollback()
	return errors.Join(err, tx.conn.restoreReadOnly(context.Background()))
}
