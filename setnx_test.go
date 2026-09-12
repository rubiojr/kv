package kv

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetNX(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	require.NoError(t, db.MDel("nx-new", "nx-persistent", "nx-expired"))
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second).In(time.FixedZone("west", -7*3600))
	original := future
	value := []byte{0, 255, 1, 0}
	stored, err := db.SetNX("nx-new", value, &future)
	require.NoError(t, err)
	require.True(t, stored)
	assert.True(t, original == future, "SetNX must not change the caller's timestamp")

	later := future.Add(time.Hour)
	stored, err = db.SetNX("nx-new", []byte("replacement"), &later)
	require.NoError(t, err)
	assert.False(t, stored)
	got, err := db.Get("nx-new")
	require.NoError(t, err)
	assert.Equal(t, value, got)
	expires, err := db.TTL("nx-new")
	require.NoError(t, err)
	require.NotNil(t, expires)
	assert.True(t, future.Equal(*expires), "a rejected write must preserve expiration")

	stored, err = db.SetNX("nx-persistent", []byte("forever"), nil)
	require.NoError(t, err)
	assert.True(t, stored)
	stored, err = db.SetNX("nx-persistent", []byte("replacement"), &future)
	require.NoError(t, err)
	assert.False(t, stored)
	expires, err = db.TTL("nx-persistent")
	require.NoError(t, err)
	assert.Nil(t, expires)

	past := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, db.Set("nx-expired", []byte("old"), &past))
	stored, err = db.SetNX("nx-expired", []byte("new"), nil)
	require.NoError(t, err)
	assert.True(t, stored)
	got, err = db.Get("nx-expired")
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), got)
	expires, err = db.TTL("nx-expired")
	require.NoError(t, err)
	assert.Nil(t, expires)

	boundary := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Set("nx-boundary", []byte("old"), &boundary))
	stored, err = db.SetNX("nx-boundary", []byte("new"), &future)
	require.NoError(t, err)
	assert.True(t, stored)
	expires, err = db.TTL("nx-boundary")
	require.NoError(t, err)
	require.NotNil(t, expires)
	assert.True(t, future.Equal(*expires))
}

func TestSetNXConcurrent(t *testing.T) {
	urn := testDSN(t)
	first := newTestDatabase(t, urn)
	second := first
	if driver != "sqlite" || first.Raw() != first.RawReader() {
		second = newTestDatabase(t, urn)
	}
	for _, state := range []string{"missing", "expired"} {
		t.Run(state, func(t *testing.T) {
			key := "nx-race-" + state
			require.NoError(t, first.Del(key))
			if state == "expired" {
				past := time.Now().UTC().Add(-time.Hour)
				require.NoError(t, first.Set(key, []byte("old"), &past))
			}
			type result struct {
				stored bool
				value  []byte
				err    error
			}
			start := make(chan struct{})
			done := make(chan result, 24)
			for i := range 24 {
				target := first
				if i%2 == 0 {
					target = second
				}
				value := fmt.Appendf(nil, "candidate-%d", i)
				go func() {
					<-start
					stored, err := target.SetNX(key, value, nil)
					done <- result{stored, value, err}
				}()
			}
			close(start)
			var winner []byte
			wins := 0
			for range 24 {
				got := <-done
				assert.NoError(t, got.err)
				if got.stored {
					wins++
					winner = got.value
				}
			}
			assert.Equal(t, 1, wins)
			got, err := first.Get(key)
			require.NoError(t, err)
			assert.Equal(t, winner, got)
		})
	}
}

func TestSetNXErrors(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	past := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, db.Set("nx-invalid", []byte("old"), &past))
	stored, err := db.SetNX("nx-invalid", nil, nil)
	assert.Error(t, err, "constraint errors must not be silently ignored")
	assert.False(t, stored)
	var value string
	require.NoError(t, db.RawReader().QueryRow("SELECT value FROM key_values WHERE `key`=?", "nx-invalid").Scan(&value))
	assert.Equal(t, "old", value, "failed replacement must preserve the expired row")
	require.NoError(t, db.Close())
	stored, err = db.SetNX("nx-closed", []byte("value"), nil)
	assert.Error(t, err)
	assert.False(t, stored)
}
