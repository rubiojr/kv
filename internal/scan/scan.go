// Package scan implements byte-wise key prefix ranges, the statements that
// read and delete them, and entry decoding, shared by the SQL backends.
package scan

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rubiojr/kv/internal/expiry"
	"github.com/rubiojr/kv/types"
)

var (
	errLimit       = errors.New("scan limit must be positive")
	errEmptyPrefix = errors.New("delete prefix must not be empty")
)

// Successor returns the least string that sorts after every string starting
// with prefix, comparing bytes, so prefix <= key < successor holds exactly for
// the keys with that prefix. Trailing 0xFF bytes cannot be incremented and are
// dropped first. It returns false for an empty prefix or one made only of 0xFF
// bytes, which have no upper bound. The result need not be valid UTF-8: "¿"
// (C2 BF) becomes C2 C0, which still sorts below "À" (C3 80).
func Successor(prefix string) (string, bool) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0xff {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// Dialect is how a backend compares keys byte-wise and decodes timestamps.
type Dialect struct {
	// Key is the key column expression. It must compare and sort byte-wise.
	Key string
	// Bound is the placeholder expression for a key bound compared with Key.
	Bound string
	// Arg converts a key bound to its query argument.
	Arg func(string) any
	// Text is the type timestamps are cast to before decoding.
	Text string
}

// keyRange renders the keys that start with prefix and sort strictly after
// after as SQL conditions to join with AND, and their arguments. An after
// before prefix does not narrow the range. The lower bound is kept even when it
// is empty and holds for every key: it steers SQLite to the key index, where
// rows are already in order, rather than the expiration index and a sort.
func (d Dialect) keyRange(prefix, after string) ([]string, []any) {
	var conditions []string
	var args []any
	if after != "" && after >= prefix {
		conditions = append(conditions, d.Key+" > "+d.Bound)
		args = append(args, d.Arg(after))
	} else {
		conditions = append(conditions, d.Key+" >= "+d.Bound)
		args = append(args, d.Arg(prefix))
	}
	if upper, ok := Successor(prefix); ok {
		conditions = append(conditions, d.Key+" < "+d.Bound)
		args = append(args, d.Arg(upper))
	}
	return conditions, args
}

// ScanQuery returns the query Scan runs and its arguments: up to limit keys
// that start with prefix, sort strictly after after, and have not expired at
// now, ordered byte-wise. limit must be positive.
func (d Dialect) ScanQuery(table, prefix, after string, limit int, now time.Time) (string, []any, error) {
	if limit <= 0 {
		return "", nil, errLimit
	}
	conditions, args := d.keyRange(prefix, after)
	conditions = append(conditions, "(expires_at IS NULL OR expires_at > ?)")
	args = append(args, now, limit)
	query := fmt.Sprintf("SELECT `key`, value, CAST(expires_at AS %s), CAST(updated_at AS %s) FROM %s WHERE %s ORDER BY %s LIMIT ?",
		d.Text, d.Text, table, strings.Join(conditions, " AND "), d.Key)
	return query, args, nil
}

// DelPrefixQuery returns the statement DelPrefix runs and its arguments: it
// deletes every row whose key starts with prefix, live or expired. prefix must
// not be empty.
func (d Dialect) DelPrefixQuery(table, prefix string) (string, []any, error) {
	if prefix == "" {
		return "", nil, errEmptyPrefix
	}
	conditions, args := d.keyRange(prefix, "")
	return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(conditions, " AND ")), args, nil
}

// Scan runs ScanQuery on db and decodes the entries.
func (d Dialect) Scan(db *sql.DB, table, prefix, after string, limit int) ([]types.Entry, error) {
	query, args, err := d.ScanQuery(table, prefix, after, limit, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // Read-only query; entries checks rows.Err on completion.
	return entries(rows)
}

// DelPrefix runs DelPrefixQuery on db and returns how many rows went.
func (d Dialect) DelPrefix(db *sql.DB, table, prefix string) (int64, error) {
	query, args, err := d.DelPrefixQuery(table, prefix)
	if err != nil {
		return 0, err
	}
	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// entries decodes rows of key, value, and the expiration and update timestamps
// cast to text, in row order. Timestamps are returned in UTC; a NULL expiration
// is a persistent key. It returns an empty slice for no rows, and nil on error.
func entries(rows *sql.Rows) ([]types.Entry, error) {
	entries := []types.Entry{}
	for rows.Next() {
		var entry types.Entry
		var expires sql.NullString
		var updated string
		if err := rows.Scan(&entry.Key, &entry.Value, &expires, &updated); err != nil {
			return nil, err
		}
		if entry.Value == nil {
			// The column is NOT NULL, but drivers may decode an empty blob as nil.
			entry.Value = []byte{}
		}
		if expires.Valid {
			deadline, err := expiry.ParseTimestamp(expires.String)
			if err != nil {
				return nil, err
			}
			deadline = deadline.UTC()
			entry.ExpiresAt = &deadline
		}
		updatedAt, err := expiry.ParseTimestamp(updated)
		if err != nil {
			return nil, err
		}
		entry.UpdatedAt = updatedAt.UTC()
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return entries, nil
}
