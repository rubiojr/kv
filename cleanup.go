package kv

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// WithCleanupInterval enables background expiration cleanup. Zero (the default)
// disables it; negative intervals are invalid. One batch runs per interval,
// starting after the first interval, with no overlapping background purges.
func WithCleanupInterval(interval time.Duration) Option {
	return func(o *options) { o.cleanupInterval = interval }
}

// WithCleanupBatchSize sets the maximum rows deleted per interval (default 1000).
// The size must be positive. This option alone does not enable cleanup.
func WithCleanupBatchSize(size int) Option {
	return func(o *options) { o.cleanupBatchSize = size }
}

// WithCleanupErrorHandler receives background purge errors. A nil handler uses
// slog.Error. The handler runs on the worker goroutine: it must return promptly
// and must not call Close. Shutdown cancellation is not reported.
func WithCleanupErrorHandler(handler func(error)) Option {
	return func(o *options) { o.cleanupError = handler }
}

type cleanupDatabase struct {
	Database
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func startCleanup(db Database, settings options) *cleanupDatabase {
	ctx, cancel := context.WithCancel(context.Background())
	d := &cleanupDatabase{Database: db, cancel: cancel, done: make(chan struct{})}
	handler := settings.cleanupError
	if handler == nil {
		handler = func(err error) { slog.Error("kv expiration cleanup failed", "error", err) }
	}
	go func() {
		defer close(d.done)
		ticker := time.NewTicker(settings.cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil {
					return
				}
				if _, err := db.PurgeExpired(ctx, settings.cleanupBatchSize); err != nil && ctx.Err() == nil {
					handler(err)
				}
			}
		}
	}()
	return d
}

// Init cannot reopen a store while its cleanup worker owns the lifecycle.
func (d *cleanupDatabase) Init(string, string) error {
	return errors.New("cannot reinitialize a store with background cleanup; use New")
}

// Close stops cleanup, waits for it to finish, and then closes the database pools.
func (d *cleanupDatabase) Close() error {
	d.closeOnce.Do(func() {
		d.cancel()
		<-d.done
		d.closeErr = d.Database.Close()
	})
	return d.closeErr
}
