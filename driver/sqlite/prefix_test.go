package sqlite

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDatabase(t *testing.T) *Database {
	t.Helper()
	db := &Database{}
	require.NoError(t, db.Init("key_values", filepath.Join(t.TempDir(), "kv.db")))
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func scanKeys(t *testing.T, db *Database, prefix string) []string {
	t.Helper()
	entries, err := db.Scan(prefix, "", 100)
	require.NoError(t, err)
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
	}
	return keys
}

// SQLite stores Go strings as they are, so keys need not be UTF-8 and can end
// in 0xFF bytes, which have no successor.
func TestPrefixHighBytes(t *testing.T) {
	db := openTestDatabase(t)
	keys := []string{"a", "a\xff", "a\xff\x00", "a\xff\xff", "b", "\xfe", "\xff", "\xff\x00", "\xff\xff", "\xff\xff\xff"}
	for _, key := range keys {
		require.NoError(t, db.Set(key, []byte("v"), nil))
	}
	for prefix, want := range map[string][]string{
		"a\xff":     {"a\xff", "a\xff\x00", "a\xff\xff"},
		"a\xff\xff": {"a\xff\xff"},
		"\xff":      {"\xff", "\xff\x00", "\xff\xff", "\xff\xff\xff"},
		"\xff\xff":  {"\xff\xff", "\xff\xff\xff"},
		"\xfe":      {"\xfe"},
		"":          keys,
	} {
		assert.Equal(t, want, scanKeys(t, db, prefix), "prefix %q", prefix)
	}
	page, err := db.Scan("\xff", "\xff\x00", 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	assert.Equal(t, []string{"\xff\xff", "\xff\xff\xff"}, []string{page[0].Key, page[1].Key})

	deleted, err := db.DelPrefix("\xff\xff")
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	deleted, err = db.DelPrefix("a\xff")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)
	assert.Equal(t, []string{"a", "b", "\xfe", "\xff", "\xff\x00"}, scanKeys(t, db, ""))
}

// Prefix ranges must stay searches on the primary key rather than table scans.
func TestPrefixUsesKeyIndex(t *testing.T) {
	db := openTestDatabase(t)
	now := time.Now().UTC()
	plan := func(query string, args []any) string {
		t.Helper()
		rows, err := db.RawReader().Query("EXPLAIN QUERY PLAN "+query, args...)
		require.NoError(t, err)
		defer rows.Close() //nolint:errcheck // Read-only query; completion errors are checked via rows.Err.
		var details []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
			details = append(details, detail)
		}
		require.NoError(t, rows.Err())
		return strings.Join(details, "; ")
	}
	for _, tc := range []struct{ prefix, after string }{{"p/", ""}, {"p/", "p/a"}, {"", "k"}, {"", ""}} {
		query, args, err := prefixes.ScanQuery(db.t, tc.prefix, tc.after, 10, now)
		require.NoError(t, err)
		got := plan(query, args)
		assert.Contains(t, got, "SEARCH key_values USING INDEX sqlite_autoindex_key_values_1 (key>", "scan %q after %q", tc.prefix, tc.after)
		assert.NotContains(t, got, "TEMP B-TREE", "ordering must come from the index")
	}
	query, args, err := prefixes.DelPrefixQuery(db.t, "p/")
	require.NoError(t, err)
	assert.Contains(t, plan(query, args), "SEARCH key_values USING INDEX sqlite_autoindex_key_values_1 (key>? AND key<?)")
}
