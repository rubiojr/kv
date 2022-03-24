//go:build integration
// +build integration

package kv

import (
	"testing"
	"time"

	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack"
)

func TestMySQL(t *testing.T) {
	db, err := New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	if err != nil {
		t.Fatal(t, err)
	}

	err = db.Set("foo", []byte("bar"), nil)
	assert.NoError(t, err)

	v, err := db.Get("foo")
	assert.NoError(t, err)
	assert.Equal(t, "bar", string(v))

	err = db.Set("stuff", []byte("staff"), nil)
	assert.NoError(t, err)

	v, err = db.Get("stuff")
	assert.NoError(t, err)
	assert.Equal(t, "staff", string(v))

	values, err := db.MGet("foo", "stuff")
	assert.NoError(t, err)

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

	t.Run("mset", func(t *testing.T) {
		values := types.KeyValues{}
		values["mset1"] = "msetv1"
		values["mset2"] = "msetv2"
		err = db.MSet(values, nil)
		assert.NoError(t, err)

		v, err := db.Get("mset1")
		assert.NoError(t, err)
		assert.Equal(t, "msetv1", string(v))

		v, err = db.Get("mset2")
		assert.NoError(t, err)
		assert.Equal(t, "msetv2", string(v))
	})

	t.Run("binary mset", func(t *testing.T) {
		b, err := msgpack.Marshal("blob")
		assert.NoError(t, err)

		err = db.MSet(types.KeyValues{"bin": b}, nil)
		assert.NoError(t, err)

		v, err = db.Get("bin")
		assert.NoError(t, err)

		var blobStr string
		err = msgpack.Unmarshal(b, &blobStr)
		assert.NoError(t, err)

		assert.Equal(t, "blob", blobStr)
	})

	t.Run("mdel", func(t *testing.T) {
		err = db.MSet(types.KeyValues{"bin": "foo"}, nil)
		assert.NoError(t, err)

		err = db.MDel("bin")
		assert.NoError(t, err)

		_, err = db.Get("bin")
		assert.Error(t, err, errors.ErrKeyNotFound)

		err = db.MSet(types.KeyValues{"bin": "foo", "bang": "bar"}, nil)
		assert.NoError(t, err)

		err = db.MDel("bin", "bang")
		assert.NoError(t, err)

		_, err = db.Get("bin")
		assert.Error(t, err, errors.ErrKeyNotFound)

		_, err = db.Get("bang")
		assert.Error(t, err, errors.ErrKeyNotFound)
	})

	t.Run("del", func(t *testing.T) {
		err = db.Set("bin", []byte("bang"), nil)
		assert.NoError(t, err)

		ok, err := db.Exists("bin")
		assert.Equal(t, true, ok)

		err = db.Del("bin")
		assert.NoError(t, err)

		_, err = db.Get("bin")
		assert.Error(t, err, errors.ErrKeyNotFound)
		ok, err = db.Exists("bin")
		assert.Equal(t, false, ok)
	})

	t.Run("mexists", func(t *testing.T) {
		values := types.KeyValues{}
		values["mexists1"] = "msetv1"
		values["mexists2"] = "msetv2"
		err := db.MSet(values, nil)
		assert.NoError(t, err)

		ok, err := db.MExists("mexists1")
		assert.NoError(t, err)
		assert.True(t, len(ok) == 1)
		assert.True(t, ok[0])

		ok, err = db.MExists("mexists1", "mexistsN", "mexists2")
		assert.NoError(t, err)
		assert.True(t, len(ok) == 3)
		assert.Equal(t, true, ok[0])
		assert.Equal(t, false, ok[1])
		assert.Equal(t, true, ok[2])

		ok, err = db.MExists()
		assert.NoError(t, err)
		assert.True(t, len(ok) == 0)
	})
}

func TestMySQLDoubleInit(t *testing.T) {
	_, err := New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	assert.NoError(t, err)

	_, err = New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	assert.NoError(t, err)
}

func TestMySQLExpiry(t *testing.T) {
	db, err := New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
	assert.NoError(t, err)

	now := time.Now()
	err = db.Set("expiry", []byte("value"), &now)
	assert.NoError(t, err)

	_, err = db.Get("expiry")
	assert.Error(t, errors.ErrKeyNotFound, err)

	later := time.Now().Add(1 * time.Minute)
	err = db.Set("expiry2", []byte("value"), &later)
	assert.NoError(t, err)

	val, err := db.Get("expiry2")
	assert.NoError(t, err)
	assert.Equal(t, "value", string(val))

	utc := time.Now().UTC().Add(1 * time.Minute)
	err = db.Set("expiry3", []byte("value"), &utc)
	assert.NoError(t, err)

	val, err = db.Get("expiry3")
	assert.NoError(t, err)
	assert.Equal(t, "value", string(val))
}
