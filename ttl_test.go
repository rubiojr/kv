package kv

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTTL(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	past := future.Add(-2 * time.Hour)
	require.NoError(t, db.Set("ttl-future", []byte("value"), &future))
	require.NoError(t, db.Set("ttl-expired", []byte("value"), &past))
	require.NoError(t, db.Set("ttl-persistent", []byte("value"), nil))

	expires, err := db.TTL("ttl-future")
	require.NoError(t, err)
	require.NotNil(t, expires)
	assert.True(t, future.Equal(*expires))
	for _, key := range []string{"ttl-expired", "ttl-persistent", "ttl-missing"} {
		expires, err := db.TTL(key)
		require.NoError(t, err)
		assert.Nil(t, expires, key)
	}

	later := future.Add(time.Hour)
	require.NoError(t, db.Set("ttl-future", []byte("new"), &later))
	expires, err = db.TTL("ttl-future")
	require.NoError(t, err)
	require.NotNil(t, expires)
	assert.True(t, later.Equal(*expires))
	require.NoError(t, db.Set("ttl-future", []byte("new"), nil))
	expires, err = db.TTL("ttl-future")
	require.NoError(t, err)
	assert.Nil(t, expires)
}

func TestMTTL(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	first := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	second := first.Add(time.Hour)
	past := first.Add(-2 * time.Hour)
	require.NoError(t, db.Set("mttl-a", []byte("A"), &first))
	require.NoError(t, db.Set("mttl-b", []byte("B"), &second))
	require.NoError(t, db.Set("mttl-expired", []byte("old"), &past))
	require.NoError(t, db.Set("mttl-persistent", []byte("forever"), nil))
	expires, err := db.MTTL("mttl-b", "mttl-missing", "mttl-a", "mttl-persistent", "mttl-expired", "mttl-b")
	require.NoError(t, err)
	assert.Equal(t, []*time.Time{&second, nil, &first, nil, nil, &second}, expires)

	require.NoError(t, db.Close())
	expires, err = db.MTTL()
	require.NoError(t, err, "empty input must not query the database")
	assert.Empty(t, expires)
	_, err = db.TTL("mttl-a")
	assert.Error(t, err, "database failures must not become nil expiration timestamps")
}

func TestTTLWhileWriterReserved(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	require.NoError(t, db.Set("ttl-reader", []byte("value"), &future))
	conn, err := db.Raw().Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	type result struct {
		expires *time.Time
		err     error
	}
	done := make(chan result, 1)
	go func() {
		expires, err := db.TTL("ttl-reader")
		done <- result{expires, err}
	}()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.NotNil(t, got.expires)
		assert.True(t, future.Equal(*got.expires))
	case <-time.After(2 * time.Second):
		t.Fatal("TTL blocked behind the writer pool")
	}
}
