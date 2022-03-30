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

const insert = "?"

type Database struct {
	t  string
	db *sql.DB
}

func (d *Database) Init(tableName, urn string) error {
	db, err := sql.Open("mysql", urn)
	if err != nil {
		return err
	}
	// Safe defaults: https://github.com/go-sql-driver/mysql/tree/90e813fe43edc87a66650b570e8362da44041a4c#usage
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)
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
	if len(keys) < 1 {
		return [][]byte{}, nil
	}

	knames, inserts := vRow(keys...)
	sql := fmt.Sprintf("SELECT `key`, value FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, inserts, time.Now().UTC())

	rows, err := d.db.Query(sql, knames...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := [][]byte{}
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return [][]byte{}, err
		}
		values = append(values, []byte(value))
	}

	return values, nil
}

func (d *Database) MDel(keys ...string) error {
	if len(keys) < 1 {
		return nil
	}

	knames, inserts := vRow(keys...)

	sql := fmt.Sprintf("DELETE FROM %s WHERE `key` IN(%s)", d.t, inserts)

	_, err := d.db.Exec(sql, knames...)
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

	knames, inserts := vRow(keys...)
	mcheck := map[string]bool{}
	for _, id := range keys {
		mcheck[id] = false
	}

	sql := fmt.Sprintf("SELECT `key` FROM %s WHERE `key` IN(%s) AND (`expires_at` IS NULL OR `expires_at` > '%s')", d.t, inserts, time.Now().UTC())

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

// Exists checks for existence of the specified key.
func (d *Database) Exists(key string) (bool, error) {
	values, err := d.MExists(key)
	if err != nil && !goerrors.Is(err, errors.ErrKeyNotFound) {
		return false, err
	}
	return err == nil && values[0], err
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
