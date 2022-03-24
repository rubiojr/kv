package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
	_ "modernc.org/sqlite"
)

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
	now := time.Now().UTC()
	args := make([]interface{}, len(keys))
	for i, id := range keys {
		args[i] = id
	}
	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(?"+strings.Repeat(",?", len(args)-1)+") AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, now)

	rows, err := d.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}

	values := [][]byte{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			panic(err)
		}
		values = append(values, []byte(value))
	}

	return values, nil
}

func (d *Database) MDel(keys ...string) error {
	const insert = "?"
	var klist []string
	values := []interface{}{}

	for _, k := range keys {
		klist = append(klist, insert)
		values = append(values, k)
	}

	inserts := strings.Join(klist, ",")

	sql := fmt.Sprintf("DELETE FROM %s WHERE `key` IN(%s)", d.t, inserts)

	_, err := d.db.Exec(sql, values...)
	return err
}

func (d *Database) Del(key string) error {
	return d.MDel(key)
}
