package kv

import (
	"errors"
	"time"

	"github.com/rubiojr/kv/driver/sqlite"
)

const TABLE_NAME = "key_values"
const MAX_KEY_LENGTH = 255
const MAX_VALUE_LENGTH = 65535

type KV struct {
	d Database
}

type Database interface {
	Init(tableName string, urn string) error
	Get(key string) ([]byte, error)
	MGet(...string) ([][]byte, error)
	Set(key string, value []byte, expireAt *time.Time) error
}

func New(driver string, urn string) (Database, error) {
	var db Database
	var err error
	switch driver {
	case "sqlite":
		db, err = &sqlite.Database{}, nil
	default:
		err = errors.New("driver not supported")
	}

	if err != nil {
		return nil, err
	}

	return db, db.Init(TABLE_NAME, "db.db")
}
