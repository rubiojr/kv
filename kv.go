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

type Database interface {
	Init(tableName string, urn string) error

	Get(key string) ([]byte, error)
	MGet(...string) ([][]byte, error)

	Set(key string, value []byte, expireAt *time.Time) error
	MSet(kvs types.KeyValues, expireAt *time.Time) error
	// SetNX stores a value only when the key is missing or expired, returning whether it was stored.
	SetNX(key string, value []byte, expireAt *time.Time) (bool, error)

	Del(key string) error
	MDel(keys ...string) error
	// PurgeExpired deletes at most batchSize expired rows and returns the count.
	// batchSize must be positive. Persistent and live keys are preserved.
	PurgeExpired(ctx context.Context, batchSize int) (int64, error)

	Exists(key string) (bool, error)
	MExists(keys ...string) ([]bool, error)

	// TTL returns the expiration timestamp, or nil for missing, expired, or persistent keys.
	TTL(key string) (*time.Time, error)
	// MTTL returns expiration timestamps in input order, including duplicates.
	MTTL(keys ...string) ([]*time.Time, error)

	// Raw returns the pool used for writes.
	Raw() *sql.DB
	// RawReader returns the pool used for reads; some backends share it with Raw.
	RawReader() *sql.DB
	// Close closes all pools owned by the store.
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
