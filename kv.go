package kv

const TABLE_NAME = "key_values"
const MAX_KEY_LENGTH = 255
const MAX_VALUE_LENGTH = 65535

type KV struct {
	d Driver
}

type Driver interface {
	Init(tableName string, urn string) error
	Get(key string) ([]byte, error)
}

func New(driver Driver) *KV {
	return &KV{d: driver}
}
