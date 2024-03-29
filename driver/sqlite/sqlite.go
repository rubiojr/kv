package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	goerrors "errors"

	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
	_ "modernc.org/sqlite"
)

const insert = "?"

type Database struct {
	t  string
	db *sql.DB
}

func (d *Database) Init(tableName, urn string) error {
	db, err := sql.Open("sqlite", urn)
	if err != nil {
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
	d.db = db
	d.t = tableName

	return err
}

func (d *Database) Raw() *sql.DB {
	return d.db
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

// MSet sets the specified hash keys to their associated values, setting them to
// expire at the specified time. Returns nil. Raises on error.
func (d *Database) MSet(kvs types.KeyValues, expiresAt *time.Time) error {
	now := time.Now().UTC()
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

func (d *Database) MGet(keys ...string) ([][]byte, error) {
	lkeys := len(keys)
	if lkeys < 1 {
		return [][]byte{}, nil
	}

	knames, inserts := vRow(keys...)
	now := time.Now().UTC()

	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, inserts, now)

	rows, err := d.db.Query(sql, knames...)
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

	return values, nil
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

	sql := fmt.Sprintf("SELECT `key` FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, inserts, now)

	rows, err := d.db.Query(sql, knames...)
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

	return values, nil
}

func (d *Database) Exists(key string) (bool, error) {
	values, err := d.MExists(key)
	if err != nil && !goerrors.Is(err, errors.ErrKeyNotFound) {
		return false, err
	}
	return err == nil && values[0], err
}
