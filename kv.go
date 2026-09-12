// Package kv provides a SQL-backed key/value store with SQLite and MySQL backends.
// Use [New] to open a [Database] and Database.Close to release its resources.
// Values are read as bytes and may have an optional expiration timestamp.
package kv

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/rubiojr/kv/driver/mysql"
	"github.com/rubiojr/kv/driver/sqlite"
	"github.com/rubiojr/kv/types"
)

const TABLE_NAME = "key_values"
const MAX_KEY_LENGTH = 255
const MAX_VALUE_LENGTH = 65535

type KV struct{}

// Database stores values by string key with optional expiration timestamps.
// Use [New] to obtain an initialized store.
//
// A key is expired when its expiration timestamp is at or before the current
// time. Reads treat expired keys as missing, but leave their rows in storage.
// Use PurgeExpired or enable [WithCleanupInterval] to remove expired rows.
// A persistent key has no expiration timestamp.
type Database interface {
	// Init configures the store to use tableName and the backend-specific data
	// source name urn. SQLite creates the table if needed; MySQL requires an
	// existing database and table. tableName must be a trusted SQL identifier.
	// New calls Init automatically. Stores with background cleanup enabled
	// reject calls to Init; open a new store instead.
	Init(tableName string, urn string) error

	// Get returns the value associated with key. A missing or expired key
	// returns nil and ErrKeyNotFound from package github.com/rubiojr/kv/errors.
	Get(key string) ([]byte, error)

	// MGet returns values for the requested keys that exist and have not expired.
	// Missing and expired keys are omitted, and duplicate keys produce only one
	// value. Result order is unspecified and does not correspond to input order.
	// With no keys, MGet returns an empty slice and no error.
	// Discard the results if an error is returned; the read may be incomplete.
	MGet(...string) ([][]byte, error)

	// Set stores value under key, replacing any existing value and expiration.
	// expireAt is an absolute timestamp; nil makes the key persistent and removes
	// any previous expiration. A timestamp in the past makes the key immediately
	// invisible to reads. Database and constraint failures return an error.
	Set(key string, value []byte, expireAt *time.Time) error

	// MSet stores all key/value pairs with the same expiration, replacing existing
	// values and expirations as Set does. Values are passed to database/sql as
	// query arguments; use driver-supported types such as string or []byte.
	// A nil or empty map is a successful no-op. Database and constraint failures
	// return an error.
	MSet(kvs types.KeyValues, expireAt *time.Time) error

	// SetNX atomically stores value only if key is missing or expired. It returns
	// true if stored, or false with no error if a live key already exists. A
	// rejected write preserves the existing value and expiration. expireAt has
	// the same meaning as in Set. Database and constraint failures return an error.
	SetNX(key string, value []byte, expireAt *time.Time) (bool, error)

	// Del deletes key, whether live or expired. A missing key is a successful no-op.
	Del(key string) error

	// MDel deletes the specified keys, whether live or expired. Missing keys are
	// ignored. With no keys, MDel is a successful no-op.
	MDel(keys ...string) error

	// PurgeExpired deletes at most batchSize expired rows and returns the count.
	// batchSize must be positive or an error is returned. Persistent and live
	// keys are preserved. The operation honors ctx cancellation and deadlines.
	// Call it repeatedly to remove more than one batch of expired rows.
	PurgeExpired(ctx context.Context, batchSize int) (int64, error)

	// Exists reports whether key exists and has not expired. A missing or expired
	// key returns false with no error.
	Exists(key string) (bool, error)

	// MExists reports whether each key exists and has not expired. Results have
	// one entry per input key, preserving input order and duplicates. Missing and
	// expired keys have false entries. With no keys, it returns an empty slice
	// and no error. Discard the results if an error is returned.
	MExists(keys ...string) ([]bool, error)

	// TTL returns key's absolute expiration timestamp, not a remaining duration.
	// Missing, expired, and persistent keys return nil with no error. Use Exists
	// to distinguish a persistent key from a missing or expired one.
	TTL(key string) (*time.Time, error)

	// MTTL returns absolute expiration timestamps with one entry per input key,
	// preserving input order and duplicates. Missing, expired, and persistent
	// keys have nil entries. With no keys, it returns an empty slice and no error.
	// Discard the results if an error is returned.
	MTTL(keys ...string) ([]*time.Time, error)

	// Raw returns the underlying SQL pool used for writes. Direct SQL bypasses
	// the store's expiration filtering. SQLite uses a single writer connection;
	// release rows, connections, and transactions before another write.
	// The store owns the pool; use Close to release it.
	Raw() *sql.DB

	// RawReader returns the underlying SQL pool used for reads. Direct SQL
	// bypasses the store's expiration filtering. SQLite may use a separate
	// query-only pool; MySQL shares the pool returned by Raw.
	// The store owns the pool; use Close to release it.
	RawReader() *sql.DB

	// Close stops and waits for background cleanup, if enabled, then closes all
	// pools owned by the store. Release any rows, connections, and transactions
	// obtained through Raw or RawReader before calling Close.
	Close() error
}

// Option configures a database before it is initialized.
type Option func(*options)

type options struct {
	sqliteDriver      string
	sqliteBusyTimeout *time.Duration
	sqliteConfigured  bool
	cleanupInterval   time.Duration
	cleanupBatchSize  int
	cleanupError      func(error)
}

// WithSQLiteDriver selects a registered database/sql driver for the SQLite backend.
// An empty name uses the default modernc driver. Import or register custom drivers
// before calling New. This option is only valid for the SQLite backend.
// File-backed stores require WAL and query_only support; initialization fails if
// the supplied driver cannot provide them.
func WithSQLiteDriver(name string) Option {
	return func(o *options) {
		o.sqliteDriver = name
		o.sqliteConfigured = true
	}
}

// WithSQLiteBusyTimeout sets the SQLite lock wait timeout on every connection.
// The default is five seconds; zero disables waiting. Resolution is milliseconds.
func WithSQLiteBusyTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.sqliteBusyTimeout = &timeout
		o.sqliteConfigured = true
	}
}

// New opens a key/value store using the selected backend and optional settings.
// driver must be "sqlite" or "mysql"; urn is the backend's data source name.
// For SQLite, urn can be a filename, a SQLite URI, or ":memory:". For MySQL,
// it is a go-sql-driver/mysql DSN, such as "user:pass@tcp(localhost:3306)/dbname".
//
// The store uses [TABLE_NAME]. SQLite creates the table if needed and requires
// WAL for file-backed databases. MySQL requires the database and table to exist;
// opening its pool does not verify connectivity. Background expiration cleanup
// is disabled unless enabled with [WithCleanupInterval].
//
// On success, the caller must close the returned store with Database.Close.
func New(driver string, urn string, opts ...Option) (Database, error) {
	settings := options{cleanupBatchSize: 1000}
	for _, option := range opts {
		option(&settings)
	}
	if settings.cleanupInterval < 0 {
		return nil, errors.New("cleanup interval must not be negative")
	}
	if settings.cleanupBatchSize <= 0 {
		return nil, errors.New("cleanup batch size must be positive")
	}
	var db Database
	var err error
	switch driver {
	case "sqlite":
		db = &sqlite.Database{DriverName: settings.sqliteDriver, BusyTimeout: settings.sqliteBusyTimeout}
		err = db.Init(TABLE_NAME, urn)
	case "mysql":
		if settings.sqliteConfigured {
			return nil, errors.New("SQLite options require the sqlite backend")
		}
		db = &mysql.Database{}
		err = db.Init(TABLE_NAME, urn)
	default:
		err = errors.New("driver not supported")
	}

	if err == nil && settings.cleanupInterval > 0 {
		db = startCleanup(db, settings)
	}
	return db, err
}
