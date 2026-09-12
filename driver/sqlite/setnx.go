package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/rubiojr/kv/internal/expiry"
)

// SetNX atomically stores value only if key is missing or expired.
// It returns false without changing a live key's value or expiration.
func (d *Database) SetNX(key string, value []byte, expiresAt *time.Time) (bool, error) {
	if expiresAt != nil {
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	tx, err := d.db.BeginTx(context.Background(), nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	// Write before reading: this acquires SQLite's writer lock without a
	// deferred read-to-write upgrade race. Only key conflicts are ignored.
	query := fmt.Sprintf("INSERT INTO %s (`key`, value, created_at, updated_at, expires_at) VALUES (?,?,?,?,?) ON CONFLICT(`key`) DO NOTHING", d.t)
	result, err := tx.Exec(query, key, value, now, now, expiresAt)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if count == 0 {
		stored, err := d.replaceExpired(tx, key, value, expiresAt)
		if err != nil || !stored {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (d *Database) replaceExpired(tx *sql.Tx, key string, value []byte, expiresAt *time.Time) (bool, error) {
	var current sql.NullString
	query := fmt.Sprintf("SELECT CAST(expires_at AS TEXT) FROM %s WHERE `key`=?", d.t)
	if err := tx.QueryRow(query, key).Scan(&current); err != nil {
		return false, err
	}
	if !current.Valid {
		return false, nil
	}
	deadline, err := expiry.ParseTimestamp(current.String)
	if err != nil {
		return false, err
	}
	now := time.Now().UTC()
	if deadline.After(now) {
		return false, nil
	}
	query = fmt.Sprintf("UPDATE %s SET value=?, created_at=?, updated_at=?, expires_at=? WHERE `key`=?", d.t)
	result, err := tx.Exec(query, value, now, now, expiresAt, key)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}
