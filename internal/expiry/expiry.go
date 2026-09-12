// Package expiry implements expiration handling shared by the SQL backends.
package expiry

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// Read returns active expiration timestamps in the requested key order.
// castType is the backend's text type, used to avoid driver-specific date decoding.
func Read(db *sql.DB, table, castType string, keys ...string) ([]*time.Time, error) {
	if len(keys) == 0 {
		return []*time.Time{}, nil
	}
	args := make([]any, len(keys))
	placeholders := make([]string, len(keys))
	for i, key := range keys {
		args[i], placeholders[i] = key, "?"
	}
	now := time.Now().UTC()
	query := fmt.Sprintf("SELECT `key`, CAST(expires_at AS %s) FROM %s WHERE `key` IN (%s)", castType, table, strings.Join(placeholders, ","))
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	expirations, err := readExpirations(rows, now)
	if err != nil {
		return nil, err
	}
	values := make([]*time.Time, len(keys))
	for i, key := range keys {
		if expires, ok := expirations[key]; ok {
			values[i] = &expires
		}
	}
	return values, nil
}

func readExpirations(rows *sql.Rows, now time.Time) (map[string]time.Time, error) {
	expirations := make(map[string]time.Time)
	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		if !value.Valid {
			continue
		}
		expires, err := ParseTimestamp(value.String)
		if err != nil {
			return nil, err
		}
		if expires.After(now) {
			expirations[key] = expires.UTC()
		}
	}
	return expirations, rows.Err()
}

// ParseTimestamp decodes SQL driver timestamp formats. Unzoned values use UTC.
func ParseTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid expiration timestamp %q", value)
}
