package sqlite

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/rubiojr/kv"
	_ "modernc.org/sqlite"
)

type Driver struct {
	t  string
	db *sql.DB
}

func (d *Driver) Init(tableName, urn string) error {
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

func (d *Driver) Get(key string) ([]byte, error) {
	v, err := d.MGet(key)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, kv.ErrKeyNotFound
	}

	return v[0], err
}

func (d *Driver) Set(key string, value []byte, expiresAt *time.Time) error {
	sql := fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES (?,?,?,?,?) ON CONFLICT(`key`) DO UPDATE SET updated_at=excluded.updated_at,value=excluded.value,expires_at=excluded.expires_at", d.t)

	_, err := d.db.Exec(sql, key, value, time.Now(), time.Now(), expiresAt)
	return err
}

func (d *Driver) MGet(keys ...string) ([][]byte, error) {
	t := time.Now().Format(time.RFC3339)
	args := make([]interface{}, len(keys))
	for i, id := range keys {
		args[i] = id
	}
	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(?"+strings.Repeat(",?", len(args)-1)+") AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, t)

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
