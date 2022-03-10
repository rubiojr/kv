package kv

import (
	"errors"
	"time"
)

const TABLE_NAME = "key_values"
const MAX_KEY_LENGTH = 255
const MAX_VALUE_LENGTH = 65535

var ErrKeyNotFound = errors.New("key not found")

type KV struct {
	d Driver
}

type Driver interface {
	Init(tableName string, urn string) error
	Get(key string) ([]byte, error)
	MGet(...string) ([][]byte, error)
	Set(key string, value []byte, expireAt *time.Time) error
}

func New(driver Driver) *KV {
	return &KV{d: driver}
}

func (k *KV) Get(key string) ([]byte, error) {
	return k.d.Get(key)
}

func (k *KV) MGet(keys ...string) ([][]byte, error) {
	return k.d.MGet(keys...)
}

func (k *KV) Set(key string, value []byte, expiresAt *time.Time) error {
	return k.d.Set(key, value, expiresAt)
}
