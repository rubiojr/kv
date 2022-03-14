package kv

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

var driver string

func TestSqlite(t *testing.T) {
	db, err := New("sqlite", "testdata/sqlite.db")
	if err != nil {
		t.Fatal(t, err)
	}

	err = db.Set("foo", []byte("bar"), nil)
	assert.NoError(t, err)

	v, err := db.Get("foo")
	assert.Equal(t, "bar", string(v))

	err = db.Set("stuff", []byte("staff"), nil)
	assert.NoError(t, err)

	v, err = db.Get("stuff")
	assert.Equal(t, "staff", string(v))

	values, err := db.MGet("foo", "stuff")

	assert.Equal(t, "bar", string(values[0]))
	assert.Equal(t, "staff", string(values[1]))

	t.Run("binary blobs", func(t *testing.T) {
		b, err := msgpack.Marshal("blob")
		assert.NoError(t, err)

		err = db.Set("bin", b, nil)
		assert.NoError(t, err)

		v, err = db.Get("bin")
		assert.NoError(t, err)

		var blobStr string
		err = msgpack.Unmarshal(b, &blobStr)
		assert.NoError(t, err)

		assert.Equal(t, "blob", blobStr)
	})
}

func TestMySQL(t *testing.T) {
	db, err := New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	if err != nil {
		t.Fatal(t, err)
	}

	err = db.Set("foo", []byte("bar"), nil)
	assert.NoError(t, err)

	v, err := db.Get("foo")
	assert.Equal(t, "bar", string(v))

	err = db.Set("stuff", []byte("staff"), nil)
	assert.NoError(t, err)

	v, err = db.Get("stuff")
	assert.Equal(t, "staff", string(v))

	values, err := db.MGet("foo", "stuff")

	assert.Equal(t, "bar", string(values[0]))
	assert.Equal(t, "staff", string(values[1]))

	t.Run("binary blobs", func(t *testing.T) {
		b, err := msgpack.Marshal("blob")
		assert.NoError(t, err)

		err = db.Set("bin", b, nil)
		assert.NoError(t, err)

		v, err = db.Get("bin")
		assert.NoError(t, err)

		var blobStr string
		err = msgpack.Unmarshal(b, &blobStr)
		assert.NoError(t, err)

		assert.Equal(t, "blob", blobStr)
	})
}

func TestMySQLDoubleInit(t *testing.T) {
	_, err := New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	assert.NoError(t, err)

	_, err = New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	assert.NoError(t, err)
}

func TestSqliteDoubleInit(t *testing.T) {
	_, err := New("sqlite", "testdata/sqlite.db")
	assert.NoError(t, err)

	_, err = New("sqlite", "testdata/sqlite.db")
	assert.NoError(t, err)
}
