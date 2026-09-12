package kv

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPurgeExpired(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	// Drain earlier tests' expired rows when the integration suite shares a table.
	for {
		n, err := db.PurgeExpired(context.Background(), 1000)
		require.NoError(t, err)
		if n == 0 {
			break
		}
	}
	past, future := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)
	for i := 0; i < 5; i++ {
		require.NoError(t, db.Set(fmt.Sprintf("purge-%d", i), []byte("old"), &past))
	}
	require.NoError(t, db.Set("purge-live", []byte("live"), &future))
	require.NoError(t, db.Set("purge-persistent", []byte("persistent"), nil))
	// Refreshing an expired row must protect it from subsequent purges.
	require.NoError(t, db.Set("purge-refreshed", []byte("old"), &past))
	require.NoError(t, db.Set("purge-refreshed", []byte("new"), &future))
	for _, size := range []int{0, -1} {
		n, err := db.PurgeExpired(context.Background(), size)
		require.Error(t, err)
		assert.Zero(t, n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := db.PurgeExpired(ctx, 2)
	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, n)
	for _, want := range []int64{2, 2, 1, 0} {
		n, err := db.PurgeExpired(context.Background(), 2)
		require.NoError(t, err)
		assert.Equal(t, want, n)
	}
	var count int
	require.NoError(t, db.Raw().QueryRow("SELECT COUNT(*) FROM key_values WHERE `key` LIKE 'purge-%'").Scan(&count))
	assert.Equal(t, 3, count, "expired rows must be physically removed")
	for key, want := range map[string]string{"purge-live": "live", "purge-persistent": "persistent", "purge-refreshed": "new"} {
		got, err := db.Get(key)
		require.NoError(t, err)
		assert.Equal(t, want, string(got))
	}
}

func TestCleanupOptions(t *testing.T) {
	for _, option := range []Option{WithCleanupInterval(-time.Second), WithCleanupBatchSize(0), WithCleanupBatchSize(-1)} {
		db, err := New("sqlite", ":memory:", option)
		require.Error(t, err)
		assert.Nil(t, db)
	}
	db, err := New("sqlite", ":memory:", WithCleanupBatchSize(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, running := db.(*cleanupDatabase)
	assert.False(t, running)
}

func TestBackgroundCleanup(t *testing.T) {
	db, err := New("sqlite", filepath.Join(t.TempDir(), "cleanup.db"), WithCleanupInterval(10*time.Millisecond), WithCleanupBatchSize(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	past := time.Now().Add(-time.Hour)
	for i := 0; i < 3; i++ {
		require.NoError(t, db.Set(fmt.Sprintf("expired-%d", i), []byte("old"), &past))
	}
	require.Eventually(t, func() bool {
		var count int
		return db.RawReader().QueryRow("SELECT COUNT(*) FROM key_values").Scan(&count) == nil && count == 0
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, db.Close())
	assert.Error(t, db.Raw().Ping())
	assert.Error(t, db.RawReader().Ping())
}

type cleanupStub struct {
	Database
	purge func(context.Context, int) (int64, error)
	close func() error
}

func (d *cleanupStub) PurgeExpired(ctx context.Context, size int) (int64, error) {
	return d.purge(ctx, size)
}

func (d *cleanupStub) Close() error { return d.close() }

func TestCleanupWorker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("purge failed")
		calls, reports, closes := 0, 0, 0
		finished := false
		stub := &cleanupStub{
			purge: func(ctx context.Context, size int) (int64, error) {
				assert.Equal(t, 7, size)
				calls++
				if calls == 1 {
					return 0, failure
				}
				<-ctx.Done()
				finished = true
				return 0, ctx.Err()
			},
			close: func() error {
				assert.True(t, finished, "worker must finish before pools close")
				closes++
				return failure
			},
		}
		db := startCleanup(stub, options{cleanupInterval: time.Second, cleanupBatchSize: 7, cleanupError: func(err error) {
			assert.ErrorIs(t, err, failure)
			reports++
		}})
		synctest.Wait()
		assert.Zero(t, calls, "no immediate sweep")
		time.Sleep(time.Second)
		synctest.Wait()
		assert.Equal(t, 1, calls)
		assert.Equal(t, 1, reports)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		assert.Equal(t, 2, calls, "retry on next tick, without overlapping a blocked purge")
		var wg sync.WaitGroup
		for i := 0; i < 3; i++ {
			wg.Go(func() { assert.ErrorIs(t, db.Close(), failure) })
		}
		wg.Wait()
		assert.Equal(t, 1, closes)
		assert.Equal(t, 1, reports, "shutdown cancellation must not reach handler")
		time.Sleep(2 * time.Second)
		assert.Equal(t, 2, calls, "no purges after Close")
	})
}
