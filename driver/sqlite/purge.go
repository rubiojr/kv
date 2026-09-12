package sqlite

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// PurgeExpired deletes at most batchSize expired rows using the writer pool.
func (d *Database) PurgeExpired(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, errors.New("purge batch size must be positive")
	}
	// A subquery works without SQLite's optional DELETE LIMIT extension.
	// Keep selection and deletion in one statement so refreshes cannot race it.
	query := fmt.Sprintf("DELETE FROM %s WHERE expires_at <= ? AND `key` IN (SELECT `key` FROM %s WHERE expires_at <= ? ORDER BY expires_at LIMIT ?)", d.t, d.t)
	now := time.Now().UTC()
	result, err := d.db.ExecContext(ctx, query, now, now, batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
