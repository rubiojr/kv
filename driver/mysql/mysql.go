package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	goerrors "errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
)

type Database struct {
	t  string
	db *sql.DB
}

func (d *Database) Init(tableName, urn string) error {
	var dbName string
	tokens := strings.Split(urn, "/")
	if len(tokens) <= 0 {
		return fmt.Errorf("invalid urn string")
	}

	dbName = tokens[len(tokens)-1]

	db, err := sql.Open("mysql", strings.TrimSuffix(urn, dbName))
	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %s
`, dbName)
	_, err = db.Exec(sql)
	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		return err
	}

	sql = fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s ("+
			"`id` bigint(20) NOT NULL AUTO_INCREMENT,"+
			"`key` varchar(255) NOT NULL,"+
			"`value` blob NOT NULL,"+
			"`created_at` datetime NOT NULL,"+
			"`updated_at` datetime NOT NULL,"+
			"`expires_at` datetime DEFAULT NULL,"+
			"PRIMARY KEY (id),"+
			"UNIQUE KEY index_key_values_on_key (`key`),"+
			"KEY index_key_values_on_expires_at (expires_at)"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8", tableName)

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

func (d *Database) MSet(kvs types.KeyValues, expiresAt *time.Time) error {
	now := time.Now()
	rowValues := []interface{}{}
	const row = "(?,?,?,?,?)"
	var rows []string

	for k, v := range kvs {
		rows = append(rows, row)
		rowValues = append(rowValues, k, v, now, now, expiresAt)
	}

	rowsStr := strings.Join(rows, ",")

	sql := fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES %s ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at),value=VALUES(value),expires_at=VALUES(expires_at)", d.t, rowsStr)

	_, err := d.db.Exec(sql, rowValues...)
	return err
}

func (d *Database) Set(key string, value []byte, expiresAt *time.Time) error {
	return d.MSet(types.KeyValues{key: value}, expiresAt)
}

func (d *Database) MGet(keys ...string) ([][]byte, error) {
	t := time.Now().UTC()
	args := make([]interface{}, len(keys))
	for i, id := range keys {
		args[i] = id
	}
	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(?"+strings.Repeat(",?", len(args)-1)+") AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, t)

	rows, err := d.db.Query(sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// MExists checks for existence of all specified keys. Booleans will be returned in
// the same order as keys are specified.
func (d *Database) MExists(keys ...string) ([]bool, error) {
	lkeys := len(keys)
	if lkeys < 1 {
		return []bool{}, nil
	}

	now := time.Now().UTC()
	args := make([]interface{}, lkeys)
	mcheck := map[string]bool{}
	for i, id := range keys {
		args[i] = id
		mcheck[id] = false
	}

	sql := fmt.Sprintf("SELECT `key` FROM %s WHERE `key` IN(?"+strings.Repeat(",?", lkeys-1)+") AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, now)

	rows, err := d.db.Query(sql, args...)
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

// Exists checks for existence of the specified key.
func (d *Database) Exists(key string) (bool, error) {
	values, err := d.MExists(key)
	if err != nil && !goerrors.Is(err, errors.ErrKeyNotFound) {
		return false, err
	}
	return err == nil && values[0], err
}
