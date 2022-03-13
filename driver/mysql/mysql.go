package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rubiojr/kv/errors"
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

func (d *Database) Set(key string, value []byte, expiresAt *time.Time) error {
	sql := fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at),value=VALUES(value),expires_at=VALUES(expires_at)", d.t)

	_, err := d.db.Exec(sql, key, value, time.Now(), time.Now(), expiresAt)
	return err
}

func (d *Database) MGet(keys ...string) ([][]byte, error) {
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
