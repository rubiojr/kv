package mysql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

// CompareAndSwap replaces a live key's value and expiration only if its value
// is byte-equal to old. MySQL reports changed rather than matched rows unless
// clientFoundRows is set, so a lone UPDATE cannot tell a matching swap to an
// identical row from a mismatch. A locking read decides instead, and the row
// lock keeps the value until the update commits.
func (d *Database) CompareAndSwap(key string, old, new []byte, expiresAt *time.Time) (bool, error) {
	if expiresAt != nil {
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	return retryDeadlocks(func() (bool, error) { return d.compareAndSwapOnce(key, old, new, expiresAt) })
}

func (d *Database) compareAndSwapOnce(key string, old, new []byte, expiresAt *time.Time) (bool, error) {
	// READ COMMITTED takes no gap locks, and its locking reads see the latest
	// committed value, so waiters for a swapped row find it changed.
	tx, err := d.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // Fallback on errors; successful paths check Commit or Rollback.
	now := time.Now().UTC()
	var found int
	query := fmt.Sprintf("SELECT 1 FROM %s WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND value = UNHEX(?) FOR UPDATE", d.t)
	err = tx.QueryRow(query, key, now, hex.EncodeToString(old)).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Rollback()
	}
	if err != nil {
		return false, err
	}
	query = fmt.Sprintf("UPDATE %s SET value=?, updated_at=?, expires_at=? WHERE `key`=?", d.t)
	if _, err := tx.Exec(query, new, now, expiresAt, key); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// CompareAndDelete deletes a live key only if its value is byte-equal to old.
// DELETE counts deleted rows whatever clientFoundRows says.
func (d *Database) CompareAndDelete(key string, old []byte) (bool, error) {
	return retryDeadlocks(func() (bool, error) { return d.compareAndDeleteOnce(key, old) })
}

func (d *Database) compareAndDeleteOnce(key string, old []byte) (bool, error) {
	// Concurrent deletes of one unique key can deadlock under REPEATABLE READ's
	// next-key locks on the delete-marked record; READ COMMITTED takes none.
	tx, err := d.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // Fallback on errors; the successful path checks Commit.
	query := fmt.Sprintf("DELETE FROM %s WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND value = UNHEX(?)", d.t)
	result, err := tx.Exec(query, key, time.Now().UTC(), hex.EncodeToString(old))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return count > 0, nil
}

// retryDeadlocks runs attempt up to three times while InnoDB picks it as a
// deadlock victim, which rolls back its whole transaction.
func retryDeadlocks(attempt func() (bool, error)) (bool, error) {
	var err error
	for i := range 3 {
		var done bool
		done, err = attempt()
		var mysqlError *mysqldriver.MySQLError
		if !errors.As(err, &mysqlError) || mysqlError.Number != 1213 {
			return done, err
		}
		if i < 2 {
			time.Sleep(time.Duration(i+1) * time.Millisecond)
		}
	}
	return false, err
}
