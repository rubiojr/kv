package sqlite

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	goerrors "errors"

	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
)

const insert = "?"

type Database struct {
	// DriverName is a registered database/sql driver. Empty selects modernc.
	DriverName string
	// BusyTimeout configures lock waiting in milliseconds. Nil defaults to five seconds.
	BusyTimeout *time.Duration

	t      string
	db     *sql.DB
	readDB *sql.DB
}

// Init opens the database and creates its table if needed. File-backed databases
// require WAL, one writer connection, and up to eight read-only connections.
// Private in-memory and temporary databases share one connection for all access.
func (d *Database) Init(tableName, urn string) (err error) {
	driverName := d.DriverName
	if driverName == "" {
		driverName = defaultDriverName
	}
	busyTimeout := 5 * time.Second
	if d.BusyTimeout != nil {
		busyTimeout = *d.BusyTimeout
	}
	db, err := openPool(driverName, urn, busyTimeout, false)
	if err != nil {
		return err
	}
	var reader *sql.DB
	defer func() {
		if err != nil {
			closePools(db, reader)
		}
	}()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// Ask SQLite rather than guessing from the filename or URI spelling.
	var filename string
	if err = db.QueryRow("SELECT file FROM pragma_database_list WHERE name='main'").Scan(&filename); err != nil {
		return err
	}
	sql := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s(
  'key' varchar(255) NOT NULL,
  'value' blob NOT NULL,
  'created_at' datetime NOT NULL,
  'updated_at' datetime NOT NULL,
  'expires_at' datetime DEFAULT NULL,
  PRIMARY KEY ('key')
);
`, tableName)

	_, err = db.Exec(sql)
	if err != nil {
		return err
	}
	reader = db
	if filename != "" || sharedCacheURI(urn) {
		reader, err = openReaderPool(driverName, urn, busyTimeout)
		if err != nil {
			return err
		}
	}
	d.db = db
	d.readDB = reader
	d.t = tableName

	return err
}

// Raw returns the single-connection writer pool. Release its rows, connections,
// and transactions before another write. Use Close to close the whole store.
func (d *Database) Raw() *sql.DB {
	return d.db
}

// RawReader returns the query-only reader pool. Private memory and temporary
// databases share the writer pool instead. Release its resources before Close.
func (d *Database) RawReader() *sql.DB {
	if d.readDB != nil {
		return d.readDB
	}
	return d.db
}

// Close closes all pools owned by this database.
func (d *Database) Close() error {
	return closePools(d.db, d.readDB)
}

func closePools(writer, reader *sql.DB) error {
	var readError, writeError error
	if reader != nil && reader != writer {
		readError = reader.Close()
	}
	if writer != nil {
		writeError = writer.Close()
	}
	return goerrors.Join(readError, writeError)
}

// sharedCacheURI recognizes SQLite's standard URI option without changing the
// DSN. Other driver-specific DSN formats remain opaque to KV.
func sharedCacheURI(dsn string) bool {
	uri, err := url.Parse(dsn)
	return err == nil && uri.Scheme == "file" && uri.Query().Get("cache") == "shared"
}

func (d *Database) Get(key string) ([]byte, error) {
	v, err := d.MGet(key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, errors.ErrKeyNotFound
	}

	return v[0], err
}

// MSet sets the specified keys to their associated values and expiration time.
// A nil expiresAt removes any existing expiration. A nil or empty kvs is a
// successful no-op. MSet returns any error encountered while writing the values.
func (d *Database) MSet(kvs types.KeyValues, expiresAt *time.Time) error {
	if len(kvs) == 0 {
		return nil
	}

	now := time.Now().UTC()
	if expiresAt != nil {
		// SQLite compares these timestamps as text, so use the same zone as reads.
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	rowValues := []interface{}{}
	const row = "(?,?,?,?,?)"
	var rows []string

	for k, v := range kvs {
		rows = append(rows, row)
		rowValues = append(rowValues, k, v, now, now, expiresAt)
	}

	rowsStr := strings.Join(rows, ",")

	sql := fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES %s ON CONFLICT(`key`) DO UPDATE SET updated_at=excluded.updated_at,value=excluded.value,expires_at=excluded.expires_at", d.t, rowsStr)

	_, err := d.db.Exec(sql, rowValues...)

	return err
}

func (d *Database) Set(key string, value []byte, expiresAt *time.Time) error {
	return d.MSet(types.KeyValues{key: value}, expiresAt)
}

// MGet retrieves values for the specified keys. Results must be discarded if an
// error is returned, since the read may be incomplete.
func (d *Database) MGet(keys ...string) ([][]byte, error) {
	lkeys := len(keys)
	if lkeys < 1 {
		return [][]byte{}, nil
	}

	knames, inserts := vRow(keys...)
	now := time.Now().UTC()

	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > ?)", d.t, inserts)
	knames = append(knames, now)

	rows, err := d.RawReader().Query(sql, knames...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := [][]byte{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return values, err
		}
		values = append(values, []byte(value))
	}

	return values, rows.Err()
}

func (d *Database) MDel(keys ...string) error {
	if len(keys) < 1 {
		return nil
	}

	values, inserts := vRow(keys...)
	sql := fmt.Sprintf("DELETE FROM %s WHERE `key` IN(%s)", d.t, inserts)

	_, err := d.db.Exec(sql, values...)
	return err
}

func (d *Database) Del(key string) error {
	return d.MDel(key)
}

func vRow(keys ...string) ([]interface{}, string) {
	var ilist []string
	knames := []interface{}{}
	for _, k := range keys {
		ilist = append(ilist, insert)
		knames = append(knames, k)
	}

	return knames, strings.Join(ilist, ",")
}

// MExists checks for existence of all specified keys. Booleans will be returned in
// the same order as keys are specified.
// Results must be discarded if an error is returned, since the read may be incomplete.
func (d *Database) MExists(keys ...string) ([]bool, error) {
	lkeys := len(keys)
	if lkeys < 1 {
		return []bool{}, nil
	}

	knames, inserts := vRow(keys...)

	now := time.Now().UTC()
	mcheck := map[string]bool{}
	for _, id := range keys {
		mcheck[id] = false
	}

	sql := fmt.Sprintf("SELECT `key` FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > ?)", d.t, inserts)
	knames = append(knames, now)

	rows, err := d.RawReader().Query(sql, knames...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := make([]bool, lkeys)

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return values, err
		}
		mcheck[key] = true
	}

	for i, k := range keys {
		values[i] = mcheck[k]
	}

	return values, rows.Err()
}

func (d *Database) Exists(key string) (bool, error) {
	values, err := d.MExists(key)
	if err != nil && !goerrors.Is(err, errors.ErrKeyNotFound) {
		return false, err
	}
	return err == nil && values[0], err
}
