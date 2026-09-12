package mysql

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
	query := fmt.Sprintf("DELETE FROM %s WHERE expires_at <= ? ORDER BY expires_at LIMIT ?", d.t)
	result, err := d.db.ExecContext(ctx, query, time.Now().UTC(), batchSize)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
