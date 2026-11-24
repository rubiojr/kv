package mysql

import (
	"encoding/hex"

	"github.com/rubiojr/kv/internal/scan"
	"github.com/rubiojr/kv/types"
)

// prefixes compares keys as binary strings, byte by byte, instead of with the
// column's case-insensitive collation. Bounds are sent hex-encoded and decoded
// with UNHEX, so they are binary strings whatever the connection character
// set, interpolation setting, or UTF-8 validity of a computed bound. The key
// index uses the column's collation, so these comparisons scan the table.
var prefixes = scan.Dialect{
	Key:   "CAST(`key` AS BINARY)",
	Bound: "UNHEX(?)",
	Arg:   func(bound string) any { return hex.EncodeToString([]byte(bound)) },
	Text:  "CHAR",
}

// Scan returns up to limit live entries whose keys start with prefix and sort
// after after, ordered byte-wise by key.
func (d *Database) Scan(prefix, after string, limit int) ([]types.Entry, error) {
	return prefixes.Scan(d.db, d.t, prefix, after, limit)
}

// DelPrefix deletes every row whose key starts with prefix, live or expired.
// An empty prefix is an error.
func (d *Database) DelPrefix(prefix string) (int64, error) {
	return prefixes.DelPrefix(d.db, d.t, prefix)
}
