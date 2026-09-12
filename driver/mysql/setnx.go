package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
)

var errSetNXExpiredConflict = errors.New("key expired during conditional insertion")

// SetNX atomically stores value only if key is missing or expired.
// It returns false without changing a live key's value or expiration.
func (d *Database) SetNX(key string, value []byte, expiresAt *time.Time) (bool, error) {
	if expiresAt != nil {
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	var err error
	for attempt := range 3 {
		var stored bool
		stored, err = d.setNXOnce(key, value, expiresAt)
		if !retrySetNX(err) {
			return stored, err
		}
		if attempt < 2 {
			time.Sleep(time.Duration(attempt+1) * time.Millisecond)
		}
	}
	return false, err
}

func retrySetNX(err error) bool {
	var mysqlError *mysqldriver.MySQLError
	return errors.Is(err, errSetNXExpiredConflict) ||
		(errors.As(err, &mysqlError) && mysqlError.Number == 1213)
}

func (d *Database) setNXOnce(key string, value []byte, expiresAt *time.Time) (bool, error) {
	// READ COMMITTED avoids gap locks on missing keys while the unique key
	// constraint arbitrates concurrent insertions.
	tx, err := d.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // Fallback on errors; successful paths check Commit or Rollback.
	now := time.Now().UTC()
	var expired bool
	query := fmt.Sprintf("SELECT expires_at IS NOT NULL AND expires_at<=? FROM %s WHERE `key`=? FOR UPDATE", d.t)
	err = tx.QueryRow(query, now, key).Scan(&expired)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// Strict INSERT preserves constraint errors and is independent of clientFoundRows.
		query = fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES (?,?,?,?,?)", d.t)
		if _, err := tx.Exec(query, key, value, now, now, expiresAt); err != nil {
			return d.setNXConflict(tx, key, err)
		}
	case err != nil:
		return false, err
	case !expired:
		return false, tx.Rollback()
	default:
		// Updating under the row lock avoids delete/reinsert deadlocks on expired keys.
		query = fmt.Sprintf("UPDATE %s SET value=?, created_at=?, updated_at=?, expires_at=? WHERE `key`=?", d.t)
		if _, err := tx.Exec(query, value, now, now, expiresAt, key); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Database) setNXConflict(tx *sql.Tx, key string, insertError error) (bool, error) {
	var duplicate *mysqldriver.MySQLError
	if !errors.As(insertError, &duplicate) || duplicate.Number != 1062 {
		return false, insertError
	}
	// A duplicate on an unrelated unique constraint is still a real error.
	var expired bool
	query := fmt.Sprintf("SELECT expires_at IS NOT NULL AND expires_at<=? FROM %s WHERE `key`=?", d.t)
	err := tx.QueryRow(query, time.Now().UTC(), key).Scan(&expired)
	if errors.Is(err, sql.ErrNoRows) {
		return false, insertError
	}
	if err != nil {
		return false, errors.Join(insertError, err)
	}
	if expired {
		return false, errSetNXExpiredConflict
	}
	return false, tx.Rollback()
}
