package sqlite

import (
	"github.com/rubiojr/kv/internal/scan"
	"github.com/rubiojr/kv/types"
)

// prefixes compares keys as text with SQLite's BINARY collation (memcmp), which
// orders UTF-8 by code point and matches the primary key index, so prefix
// ranges are index searches. Bounds must be text: SQLite sorts every BLOB
// after every TEXT value, and keys are stored as text.
var prefixes = scan.Dialect{
	Key:   "`key` COLLATE BINARY",
	Bound: "?",
	Arg:   func(bound string) any { return bound },
	Text:  "TEXT",
}

// Scan returns up to limit live entries whose keys start with prefix and sort
// after after, ordered byte-wise by key, using the reader pool.
func (d *Database) Scan(prefix, after string, limit int) ([]types.Entry, error) {
	return prefixes.Scan(d.RawReader(), d.t, prefix, after, limit)
}

// DelPrefix deletes every row whose key starts with prefix, live or expired,
// using the writer pool. An empty prefix is an error.
func (d *Database) DelPrefix(prefix string) (int64, error) {
	return prefixes.DelPrefix(d.db, d.t, prefix)
}
