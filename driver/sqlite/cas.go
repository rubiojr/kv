package sqlite

import (
	"fmt"
	"time"
)

// CompareAndSwap replaces a live key's value and expiration only if its value
// is byte-equal to old. One UPDATE checks and writes, so it is atomic, and
// SQLite counts matched rows even when the written values are unchanged.
func (d *Database) CompareAndSwap(key string, old, new []byte, expiresAt *time.Time) (bool, error) {
	if expiresAt != nil {
		// SQLite compares these timestamps as text, so use the same zone as reads.
		utc := expiresAt.UTC()
		expiresAt = &utc
	}
	now := time.Now().UTC()
	// MSet may have stored the value as TEXT; compare its bytes instead.
	query := fmt.Sprintf("UPDATE %s SET value=?, updated_at=?, expires_at=? WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND CAST(value AS BLOB) = ?", d.t)
	result, err := d.db.Exec(query, new, now, expiresAt, key, now, blob(old))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}

// CompareAndDelete deletes a live key only if its value is byte-equal to old,
// in one statement.
func (d *Database) CompareAndDelete(key string, old []byte) (bool, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND CAST(value AS BLOB) = ?", d.t)
	result, err := d.db.Exec(query, key, time.Now().UTC(), blob(old))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count > 0, err
}

// blob binds nil as an empty value rather than NULL, which equals nothing.
func blob(value []byte) []byte {
	if value == nil {
		return []byte{}
	}
	return value
}
