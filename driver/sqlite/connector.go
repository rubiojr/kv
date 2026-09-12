package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"time"
)

func openPool(name, dsn string, busyTimeout time.Duration, readOnly bool) (*sql.DB, error) {
	if busyTimeout < 0 || busyTimeout.Milliseconds() > math.MaxInt32 {
		return nil, fmt.Errorf("sqlite: busy timeout must be between zero and %d milliseconds", math.MaxInt32)
	}
	// database/sql has no registered-driver lookup. Obtain it from an unopened
	// pool, then close that pool before creating the configured connector.
	probe, err := sql.Open(name, dsn)
	if err != nil {
		return nil, err
	}
	registered := probe.Driver()
	if err := probe.Close(); err != nil {
		return nil, err
	}
	var base driver.Connector = &dsnConnector{registered: registered, dsn: dsn}
	if contextual, ok := registered.(driver.DriverContext); ok {
		base, err = contextual.OpenConnector(dsn)
		if err != nil {
			return nil, err
		}
	}
	return sql.OpenDB(&configuredConnector{Connector: base, busyTimeout: busyTimeout, readOnly: readOnly}), nil
}

type dsnConnector struct {
	registered driver.Driver
	dsn        string
}

func (c *dsnConnector) Driver() driver.Driver { return c.registered }

func (c *dsnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.registered.Open(c.dsn)
}

type configuredConnector struct {
	driver.Connector
	busyTimeout time.Duration
	readOnly    bool
}

func (c *configuredConnector) Close() error {
	if closer, ok := c.Connector.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (c *configuredConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if err := configureConnection(ctx, conn, c.busyTimeout, c.readOnly); err != nil {
		return nil, errors.Join(err, conn.Close())
	}
	if c.readOnly {
		return &readerConnection{Conn: conn}, nil
	}
	return conn, nil
}

func openReaderPool(name, dsn string, busyTimeout time.Duration) (*sql.DB, error) {
	reader, err := openPool(name, dsn, busyTimeout, true)
	if err != nil {
		return nil, err
	}
	reader.SetMaxOpenConns(8)
	reader.SetMaxIdleConns(8)
	// Validate BYOD reader setup now, rather than failing the first Get.
	if err := reader.Ping(); err != nil {
		return nil, errors.Join(err, reader.Close())
	}
	return reader, nil
}

func configureConnection(ctx context.Context, conn driver.Conn, busyTimeout time.Duration, readOnly bool) error {
	if err := execConnection(ctx, conn, fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeout.Milliseconds())); err != nil {
		return fmt.Errorf("sqlite busy timeout: %w", err)
	}
	filename, err := queryConnection(ctx, conn, "SELECT file FROM pragma_database_list WHERE name='main'")
	if err != nil {
		return fmt.Errorf("sqlite database metadata: %w", err)
	}
	if filename != "" {
		if err := configureWAL(ctx, conn, readOnly); err != nil {
			return err
		}
	}
	if readOnly {
		return configureReadOnly(ctx, conn)
	}
	return nil
}

func configureWAL(ctx context.Context, conn driver.Conn, readOnly bool) error {
	query := "PRAGMA journal_mode=WAL"
	if readOnly {
		query = "PRAGMA journal_mode" // Readers verify WAL without changing persistent state.
	}
	mode, err := queryConnection(ctx, conn, query)
	if err != nil {
		return fmt.Errorf("sqlite WAL: %w", err)
	}
	if mode != "wal" {
		return fmt.Errorf("sqlite: WAL unavailable (journal mode %q)", mode)
	}
	return nil
}

func configureReadOnly(ctx context.Context, conn driver.Conn) error {
	if err := execConnection(ctx, conn, "PRAGMA query_only=ON"); err != nil {
		return fmt.Errorf("sqlite read-only setup: %w", err)
	}
	mode, err := queryConnection(ctx, conn, "PRAGMA query_only")
	if err != nil {
		return fmt.Errorf("sqlite read-only verification: %w", err)
	}
	if mode != "1" {
		return fmt.Errorf("sqlite: read-only mode unavailable (query_only %q)", mode)
	}
	return nil
}

func prepareConnection(ctx context.Context, conn driver.Conn, query string) (driver.Stmt, error) {
	if contextual, ok := conn.(driver.ConnPrepareContext); ok {
		return contextual.PrepareContext(ctx, query)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return conn.Prepare(query)
}

func execConnection(ctx context.Context, conn driver.Conn, query string) (err error) {
	stmt, err := prepareConnection(ctx, conn, query)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := stmt.Close(); err == nil {
			err = closeErr
		}
	}()
	if contextual, ok := stmt.(driver.StmtExecContext); ok {
		_, err = contextual.ExecContext(ctx, nil)
	} else {
		//lint:ignore SA1019 Required fallback for drivers without StmtExecContext.
		_, err = stmt.Exec(nil) //nolint:staticcheck // SA1019: compatibility with drivers without StmtExecContext.
	}
	return err
}

// queryConnection reads the scalar result of an initialization PRAGMA/query.
func queryConnection(ctx context.Context, conn driver.Conn, query string) (result string, err error) {
	stmt, err := prepareConnection(ctx, conn, query)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := stmt.Close(); err == nil {
			err = closeErr
		}
	}()
	var rows driver.Rows
	if contextual, ok := stmt.(driver.StmtQueryContext); ok {
		rows, err = contextual.QueryContext(ctx, nil)
	} else {
		//lint:ignore SA1019 Required fallback for drivers without StmtQueryContext.
		rows, err = stmt.Query(nil) //nolint:staticcheck // SA1019: compatibility with drivers without StmtQueryContext.
	}
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := rows.Close(); err == nil {
			err = closeErr
		}
	}()
	values := make([]driver.Value, len(rows.Columns()))
	if err := rows.Next(values); err != nil {
		return "", err
	}
	if len(values) != 1 {
		return "", fmt.Errorf("sqlite: expected one initialization column, got %d", len(values))
	}
	switch value := values[0].(type) {
	case nil:
		return "", nil
	case []byte:
		return string(value), nil
	default:
		return fmt.Sprint(value), nil
	}
}
