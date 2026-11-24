package kv

import (
	"fmt"
	"testing"
	"time"

	"github.com/rubiojr/kv/errors"
	"github.com/rubiojr/kv/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func requireValue(t *testing.T, db Database, key string, want []byte) {
	t.Helper()
	got, err := db.Get(key)
	require.NoError(t, err, key)
	assert.Equal(t, want, got, key)
}

func TestCompareAndSwap(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas/")
	require.NoError(t, err)
	old := []byte{0, 0xff, 'o', 'l', 'd'}
	require.NoError(t, db.Set("cas/key", old, nil))

	swapped, err := db.CompareAndSwap("cas/key", []byte("wrong"), []byte("never"), nil)
	require.NoError(t, err)
	assert.False(t, swapped)
	requireValue(t, db, "cas/key", old)

	swapped, err = db.CompareAndSwap("cas/key", old[:4], []byte("never"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "a prefix of the value is not equal to it")
	requireValue(t, db, "cas/key", old)

	swapped, err = db.CompareAndSwap("cas/key", old, []byte("new"), nil)
	require.NoError(t, err)
	assert.True(t, swapped)
	requireValue(t, db, "cas/key", []byte("new"))

	swapped, err = db.CompareAndSwap("cas/key", old, []byte("again"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "the old value is gone after a swap")
	requireValue(t, db, "cas/key", []byte("new"))

	swapped, err = db.CompareAndSwap("cas/key", []byte("new"), []byte("new"), nil)
	require.NoError(t, err)
	assert.True(t, swapped, "swapping in an identical value still matches")

	swapped, err = db.CompareAndSwap("cas/key", []byte("NEW"), []byte("upper"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "values compare case-sensitively")
	requireValue(t, db, "cas/key", []byte("new"))

	swapped, err = db.CompareAndSwap("cas/missing", nil, []byte("value"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "CompareAndSwap must not create missing keys")
	exists, err := db.Exists("cas/missing")
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestCompareAndSwapUpdatedAt(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas-updated/")
	require.NoError(t, err)
	require.NoError(t, db.Set("cas-updated/key", []byte("old"), nil))
	stale := time.Now().UTC().Add(-time.Hour).Truncate(time.Second)
	_, err = db.Raw().Exec("UPDATE key_values SET updated_at=? WHERE `key`=?", stale, "cas-updated/key")
	require.NoError(t, err)
	before := time.Now().UTC().Truncate(time.Second)

	swapped, err := db.CompareAndSwap("cas-updated/key", []byte("old"), []byte("new"), nil)
	require.NoError(t, err)
	require.True(t, swapped)
	entries, err := db.Scan("cas-updated/key", "", 1)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.False(t, entries[0].UpdatedAt.Before(before), "updated at %v, before the swap at %v", entries[0].UpdatedAt, before)
}

func TestCompareAndSwapExpiration(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas-ttl/")
	require.NoError(t, err)
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second).In(time.FixedZone("west", -7*3600))
	original := future
	later := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Second)
	past := time.Now().UTC().Add(-time.Hour)

	require.NoError(t, db.Set("cas-ttl/key", []byte("v1"), nil))
	swapped, err := db.CompareAndSwap("cas-ttl/key", []byte("v1"), []byte("v2"), &future)
	require.NoError(t, err)
	require.True(t, swapped)
	assert.True(t, original == future, "CompareAndSwap must not change the caller's timestamp") //nolint:staticcheck // QF1009: compare the full value to detect mutation, including location.
	expires, err := db.TTL("cas-ttl/key")
	require.NoError(t, err)
	require.NotNil(t, expires, "a swap sets the new expiration")
	assert.True(t, future.Equal(*expires))

	swapped, err = db.CompareAndSwap("cas-ttl/key", []byte("v2"), []byte("v3"), &later)
	require.NoError(t, err)
	require.True(t, swapped)
	expires, err = db.TTL("cas-ttl/key")
	require.NoError(t, err)
	require.NotNil(t, expires)
	assert.True(t, later.Equal(*expires), "a swap replaces the expiration")

	swapped, err = db.CompareAndSwap("cas-ttl/key", []byte("wrong"), []byte("v4"), nil)
	require.NoError(t, err)
	require.False(t, swapped)
	expires, err = db.TTL("cas-ttl/key")
	require.NoError(t, err)
	require.NotNil(t, expires, "a rejected swap keeps the expiration")
	assert.True(t, later.Equal(*expires))

	swapped, err = db.CompareAndSwap("cas-ttl/key", []byte("v3"), []byte("v4"), nil)
	require.NoError(t, err)
	require.True(t, swapped)
	expires, err = db.TTL("cas-ttl/key")
	require.NoError(t, err)
	assert.Nil(t, expires, "a nil expiration makes the key persistent")
	requireValue(t, db, "cas-ttl/key", []byte("v4"))

	swapped, err = db.CompareAndSwap("cas-ttl/key", []byte("v4"), []byte("gone"), &past)
	require.NoError(t, err)
	require.True(t, swapped)
	_, err = db.Get("cas-ttl/key")
	assert.ErrorIs(t, err, errors.ErrKeyNotFound, "a past expiration hides the key, as in Set")

	require.NoError(t, db.Set("cas-ttl/expired", []byte("old"), &past))
	swapped, err = db.CompareAndSwap("cas-ttl/expired", []byte("old"), []byte("revived"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "expired keys are missing")
	_, err = db.Get("cas-ttl/expired")
	assert.ErrorIs(t, err, errors.ErrKeyNotFound)
	var stored []byte
	require.NoError(t, db.RawReader().QueryRow("SELECT value FROM key_values WHERE `key`=?", "cas-ttl/expired").Scan(&stored))
	assert.Equal(t, []byte("old"), stored, "a rejected swap leaves the expired row alone")
}

func TestCompareAndSwapStringValue(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas-text/")
	require.NoError(t, err)
	// SQLite stores string arguments as TEXT, which never equals a BLOB.
	require.NoError(t, db.MSet(types.KeyValues{"cas-text/a": "señor", "cas-text/b": "señor", "cas-text/empty": ""}, nil))

	swapped, err := db.CompareAndSwap("cas-text/a", []byte("Señor"), []byte("no"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "text values still compare byte-wise")
	swapped, err = db.CompareAndSwap("cas-text/a", []byte("señor"), []byte("yes"), nil)
	require.NoError(t, err)
	assert.True(t, swapped)
	requireValue(t, db, "cas-text/a", []byte("yes"))

	deleted, err := db.CompareAndDelete("cas-text/b", []byte("senor"))
	require.NoError(t, err)
	assert.False(t, deleted)
	deleted, err = db.CompareAndDelete("cas-text/b", []byte("señor"))
	require.NoError(t, err)
	assert.True(t, deleted)

	swapped, err = db.CompareAndSwap("cas-text/empty", nil, []byte("filled"), nil)
	require.NoError(t, err)
	assert.True(t, swapped, "nil old matches an empty stored value")
	requireValue(t, db, "cas-text/empty", []byte("filled"))
}

func TestCompareAndSwapEmptyValues(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas-empty/")
	require.NoError(t, err)
	require.NoError(t, db.Set("cas-empty/key", []byte{}, nil))

	swapped, err := db.CompareAndSwap("cas-empty/key", []byte{0}, []byte("x"), nil)
	require.NoError(t, err)
	assert.False(t, swapped, "a zero byte is not an empty value")
	swapped, err = db.CompareAndSwap("cas-empty/key", []byte{}, []byte("x"), nil)
	require.NoError(t, err)
	assert.True(t, swapped)
	swapped, err = db.CompareAndSwap("cas-empty/key", []byte("x"), []byte{}, nil)
	require.NoError(t, err)
	assert.True(t, swapped)
	requireValue(t, db, "cas-empty/key", []byte{})
	deleted, err := db.CompareAndDelete("cas-empty/key", nil)
	require.NoError(t, err)
	assert.True(t, deleted, "nil old matches an empty stored value")
}

func TestCompareAndDelete(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cad/")
	require.NoError(t, err)
	value := []byte{1, 0, 0xfe, 'v'}
	future := time.Now().UTC().Add(time.Hour)
	past := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, db.Set("cad/key", value, &future))
	require.NoError(t, db.Set("cad/expired", value, &past))

	deleted, err := db.CompareAndDelete("cad/key", []byte{1, 0, 0xfe, 'V'})
	require.NoError(t, err)
	assert.False(t, deleted)
	requireValue(t, db, "cad/key", value)

	deleted, err = db.CompareAndDelete("cad/key", value)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Zero(t, storedCount(t, db, "cad/key"))

	deleted, err = db.CompareAndDelete("cad/key", value)
	require.NoError(t, err)
	assert.False(t, deleted, "a deleted key is missing")

	deleted, err = db.CompareAndDelete("cad/expired", value)
	require.NoError(t, err)
	assert.False(t, deleted, "expired keys are missing")
	assert.Equal(t, 1, storedCount(t, db, "cad/expired"), "a rejected delete leaves the expired row alone")
}

func TestCompareAndSwapErrors(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("cas-err/")
	require.NoError(t, err)
	require.NoError(t, db.Set("cas-err/key", []byte("old"), nil))
	swapped, err := db.CompareAndSwap("cas-err/key", []byte("old"), nil, nil)
	assert.Error(t, err, "constraint errors must not be silently ignored")
	assert.False(t, swapped)
	requireValue(t, db, "cas-err/key", []byte("old"))

	require.NoError(t, db.Close())
	swapped, err = db.CompareAndSwap("cas-err/key", []byte("old"), []byte("new"), nil)
	assert.Error(t, err)
	assert.False(t, swapped)
	deleted, err := db.CompareAndDelete("cas-err/key", []byte("old"))
	assert.Error(t, err)
	assert.False(t, deleted)
	entries, err := db.Scan("cas-err/", "", 1)
	assert.Error(t, err)
	assert.Nil(t, entries)
	count, err := db.DelPrefix("cas-err/")
	assert.Error(t, err)
	assert.Zero(t, count)
}

func TestCompareAndSwapConcurrent(t *testing.T) {
	urn := testDSN(t)
	first := newTestDatabase(t, urn)
	// A second store on the same database contends through the database's locks
	// too. Private memory databases share one connection and cannot be reopened.
	second := first
	if driver != "sqlite" || first.Raw() != first.RawReader() {
		second = newTestDatabase(t, urn)
	}
	const workers = 24
	for _, operation := range []string{"swap", "delete"} {
		t.Run(operation, func(t *testing.T) {
			key := "cas-race/" + operation
			require.NoError(t, first.Set(key, []byte("old"), nil))
			type result struct {
				won   bool
				value []byte
				err   error
			}
			start := make(chan struct{})
			done := make(chan result, workers)
			for i := range workers {
				target := first
				if i%2 == 0 {
					target = second
				}
				value := fmt.Appendf(nil, "candidate-%d", i)
				go func() {
					<-start
					var won bool
					var err error
					if operation == "swap" {
						won, err = target.CompareAndSwap(key, []byte("old"), value, nil)
					} else {
						won, err = target.CompareAndDelete(key, []byte("old"))
					}
					done <- result{won, value, err}
				}()
			}
			close(start)
			var winner []byte
			wins := 0
			for range workers {
				got := <-done
				assert.NoError(t, got.err)
				if got.won {
					wins++
					winner = got.value
				}
			}
			assert.Equal(t, 1, wins)
			if operation == "swap" {
				requireValue(t, first, key, winner)
			} else {
				assert.Zero(t, storedCount(t, first, key))
			}
		})
	}
}
