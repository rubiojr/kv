package kv

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/rubiojr/kv/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seed stores persistent keys with their names as values, after deleting
// earlier rows under namespace, since MySQL suites share one table.
func seed(t *testing.T, db Database, namespace string, keys ...string) {
	t.Helper()
	_, err := db.DelPrefix(namespace)
	require.NoError(t, err)
	for _, key := range keys {
		require.NoError(t, db.Set(key, []byte(key), nil))
	}
}

func scanKeys(t *testing.T, db Database, prefix, after string, limit int) []string {
	t.Helper()
	entries, err := db.Scan(prefix, after, limit)
	require.NoError(t, err)
	require.NotNil(t, entries, "Scan must return an empty slice, not nil")
	keys := make([]string, len(entries))
	for i, entry := range entries {
		keys[i] = entry.Key
		assert.Equal(t, []byte(entry.Key), entry.Value, "seeded values are their keys")
	}
	return keys
}

// scanAll pages through prefix limit keys at a time, as callers do, and fails
// rather than loop forever if pages stop advancing.
func scanAll(t *testing.T, db Database, prefix string, limit int) []string {
	t.Helper()
	var keys []string
	after := ""
	for range 1000 {
		// Not scanKeys: with an empty prefix, pages hold other tests' keys too.
		page, err := db.Scan(prefix, after, limit)
		require.NoError(t, err)
		for _, entry := range page {
			keys = append(keys, entry.Key)
		}
		if len(page) < limit {
			return keys
		}
		after = page[len(page)-1].Key
	}
	require.FailNow(t, "paging did not finish", "prefix %q stuck after %q", prefix, after)
	return nil
}

// storedCount counts rows for keys, live or expired, without the store's filters.
func storedCount(t *testing.T, db Database, keys ...string) int {
	t.Helper()
	args := make([]any, len(keys))
	marks := make([]byte, 0, 2*len(keys))
	for i, key := range keys {
		args[i] = key
		if i > 0 {
			marks = append(marks, ',')
		}
		marks = append(marks, '?')
	}
	var count int
	query := fmt.Sprintf("SELECT COUNT(*) FROM key_values WHERE `key` IN (%s)", marks)
	require.NoError(t, db.RawReader().QueryRow(query, args...).Scan(&count))
	return count
}

func TestScanPaging(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	// Inserted out of order. "page", "pages/x" and "page0", the prefix's upper
	// bound, share text but not the prefix.
	seed(t, db, "page", "page/03", "page/01", "page/05", "page/02", "page/04", "page", "pages/x", "page0")

	assert.Equal(t, []string{"page/01", "page/02"}, scanKeys(t, db, "page/", "", 2))
	assert.Equal(t, []string{"page/03", "page/04"}, scanKeys(t, db, "page/", "page/02", 2))
	assert.Equal(t, []string{"page/05"}, scanKeys(t, db, "page/", "page/04", 2))
	assert.Empty(t, scanKeys(t, db, "page/", "page/05", 2))

	assert.Equal(t, []string{"page/01", "page/02", "page/03", "page/04", "page/05"}, scanAll(t, db, "page/", 2))

	assert.Equal(t, []string{"page/03", "page/04", "page/05"}, scanKeys(t, db, "page/", "page/025", 10), "after need not be a stored key")
	assert.Len(t, scanKeys(t, db, "page/", "a", 10), 5, "after before the prefix must not narrow the range")
	assert.Empty(t, scanKeys(t, db, "page/", "page0", 10), "after beyond the prefix leaves nothing")
	assert.Len(t, scanKeys(t, db, "page/", "", 100), 5, "a limit above the matches returns them all")
	assert.Equal(t, []string{"page/01"}, scanKeys(t, db, "page/", "", 1))
	assert.Empty(t, scanKeys(t, db, "nothing-here/", "", 10))

	for _, limit := range []int{0, -1} {
		entries, err := db.Scan("page/", "", limit)
		assert.Error(t, err, "limit %d", limit)
		assert.Nil(t, entries)
	}
}

func TestScanByteOrder(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	seed(t, db, "order/", "order/a", "order/é", "order/C", "order/z", "order/B")
	// Bytes, not a case- or accent-insensitive collation: B C a z é (C3 A9).
	assert.Equal(t, []string{"order/B", "order/C", "order/a", "order/z", "order/é"}, scanKeys(t, db, "order/", "", 10))
	assert.Equal(t, []string{"order/a", "order/z", "order/é"}, scanKeys(t, db, "order/", "order/C", 10))
}

func TestScanAllKeys(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	past := time.Now().UTC().Add(-time.Hour)
	seed(t, db, "all/", "all/one", "all/two")
	require.NoError(t, db.Set("all/expired", []byte("all/expired"), &past))

	// Other tests may share the table, so compare with every live stored key.
	rows, err := db.RawReader().Query("SELECT `key` FROM key_values WHERE expires_at IS NULL OR expires_at > ?", time.Now().UTC())
	require.NoError(t, err)
	var want []string
	for rows.Next() {
		var key string
		require.NoError(t, rows.Scan(&key))
		want = append(want, key)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	slices.Sort(want) // Go compares strings byte-wise.

	got := scanAll(t, db, "", 3)
	assert.Equal(t, want, got, "an empty prefix pages through every live key in byte order")
	assert.Contains(t, got, "all/one")
	assert.NotContains(t, got, "all/expired")
}

func TestScanLiteralPrefixes(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	seed(t, db, "lit/", "lit/a%b", "lit/a_c", "lit/abc", "lit/aXc", `lit/a\d`, "lit/a*e", "lit/a?f", "lit/a[g]", "lit/A%h")
	for prefix, want := range map[string][]string{
		"lit/a%": {"lit/a%b"},
		"lit/a_": {"lit/a_c"},
		`lit/a\`: {`lit/a\d`},
		"lit/a*": {"lit/a*e"},
		"lit/a?": {"lit/a?f"},
		"lit/a[": {"lit/a[g]"},
		"lit/A":  {"lit/A%h"},
		"lit/a":  {"lit/a%b", "lit/a*e", "lit/a?f", "lit/aXc", "lit/a[g]", `lit/a\d`, "lit/a_c", "lit/abc"},
	} {
		assert.Equal(t, want, scanKeys(t, db, prefix, "", 20), "prefix %q", prefix)
	}

	for _, prefix := range []string{"lit/a%", "lit/a_", `lit/a\`, "lit/a*", "lit/a?", "lit/a["} {
		deleted, err := db.DelPrefix(prefix)
		require.NoError(t, err)
		assert.Equal(t, int64(1), deleted, "prefix %q", prefix)
	}
	assert.Equal(t, []string{"lit/A%h", "lit/aXc", "lit/abc"}, scanKeys(t, db, "lit/", "", 20))
}

func TestScanCaseSensitive(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	// Distinct suffixes: MySQL's case-insensitive unique key would merge "Ab/x" and "ab/x".
	seed(t, db, "case/", "case/Ab/upper", "case/ab/lower", "case/AB/both")
	assert.Equal(t, []string{"case/Ab/upper"}, scanKeys(t, db, "case/Ab", "", 10))
	assert.Equal(t, []string{"case/ab/lower"}, scanKeys(t, db, "case/ab", "", 10))
	assert.Equal(t, []string{"case/ab/lower"}, scanKeys(t, db, "case/a", "", 10))
	assert.Equal(t, []string{"case/AB/both", "case/Ab/upper"}, scanKeys(t, db, "case/A", "", 10))

	deleted, err := db.DelPrefix("case/A")
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	assert.Equal(t, []string{"case/ab/lower"}, scanKeys(t, db, "case/", "", 10))
}

func TestScanNonASCII(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	keys := []string{"ñ/uno", "ñ/dos", "n/tres", "ño/cuatro", "ò/cinco", "日本/東京", "日本/大阪", "日本語/x", "日/y", "旦/z", "¿qué", "À/x"}
	for _, key := range keys {
		_, err := db.DelPrefix(key)
		require.NoError(t, err)
	}
	seed(t, db, "ñ/", keys...)
	for prefix, want := range map[string][]string{
		"ñ/":   {"ñ/dos", "ñ/uno"},
		"ñ":    {"ñ/dos", "ñ/uno", "ño/cuatro"}, // excludes ò (C3 B2), ñ's successor
		"日本/":  {"日本/大阪", "日本/東京"},
		"日本":   {"日本/大阪", "日本/東京", "日本語/x"},
		"日":    {"日/y", "日本/大阪", "日本/東京", "日本語/x"}, // excludes 旦, the next code point
		"¿":    {"¿qué"},                           // C2 BF: its bound C2 C0 is not valid UTF-8
		"¿qué": {"¿qué"},
	} {
		assert.Equal(t, want, scanKeys(t, db, prefix, "", 20), "prefix %q", prefix)
	}

	deleted, err := db.DelPrefix("ñ/")
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)
	deleted, err = db.DelPrefix("日本")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)
	assert.Equal(t, 7, storedCount(t, db, keys...), "only the matching keys may go")
	assert.Equal(t, []string{"ño/cuatro"}, scanKeys(t, db, "ñ", "", 10))
}

func TestScanFullKeyPrefix(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	// "fulm" is the upper bound of prefix "full", and "full/chile" of "full/child".
	seed(t, db, "ful", "full", "full/child", "full/chile", "fully", "ful", "fulm")
	assert.Equal(t, []string{"full", "full/child", "full/chile", "fully"}, scanKeys(t, db, "full", "", 10))
	assert.Equal(t, []string{"full/child", "full/chile", "fully"}, scanKeys(t, db, "full", "full", 10))
	assert.Equal(t, []string{"full/child"}, scanKeys(t, db, "full/child", "", 10))

	deleted, err := db.DelPrefix("full/child")
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	deleted, err = db.DelPrefix("full/child")
	require.NoError(t, err)
	assert.Zero(t, deleted, "nothing left to match")
	assert.Equal(t, []string{"ful", "full", "full/chile", "fully", "fulm"}, scanKeys(t, db, "ful", "", 10))
	deleted, err = db.DelPrefix("full")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)
	assert.Equal(t, []string{"ful", "fulm"}, scanKeys(t, db, "ful", "", 10))
}

func TestScanExpiration(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	past := time.Now().UTC().Add(-time.Hour)
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	seed(t, db, "exp/", "exp/persistent")
	require.NoError(t, db.Set("exp/expired", []byte("exp/expired"), &past))
	require.NoError(t, db.Set("exp/live", []byte("exp/live"), &future))

	assert.Equal(t, []string{"exp/live", "exp/persistent"}, scanKeys(t, db, "exp/", "", 10))
	assert.Equal(t, 3, storedCount(t, db, "exp/expired", "exp/live", "exp/persistent"), "Scan must not delete expired rows")

	deleted, err := db.DelPrefix("exp/")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted, "DelPrefix removes expired rows too")
	assert.Zero(t, storedCount(t, db, "exp/expired", "exp/live", "exp/persistent"))
}

func TestScanEntries(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	_, err := db.DelPrefix("entry/")
	require.NoError(t, err)
	future := time.Now().UTC().Add(time.Hour).Truncate(time.Second).In(time.FixedZone("west", -7*3600))
	binary := []byte{0, 0xff, 'a', 0, 0xfe}
	before := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, db.Set("entry/binary", binary, &future))
	require.NoError(t, db.MSet(types.KeyValues{"entry/text": "plain text"}, nil))
	require.NoError(t, db.Set("entry/empty", []byte{}, nil))
	after := time.Now().UTC().Add(time.Second) // MySQL DATETIME rounds to seconds.

	entries, err := db.Scan("entry/", "", 10)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, "entry/binary", entries[0].Key)
	assert.Equal(t, binary, entries[0].Value)
	require.NotNil(t, entries[0].ExpiresAt)
	assert.True(t, future.Equal(*entries[0].ExpiresAt), "got %v, want %v", entries[0].ExpiresAt, future)
	assert.Equal(t, time.UTC, entries[0].ExpiresAt.Location())

	assert.Equal(t, "entry/empty", entries[1].Key)
	assert.Equal(t, []byte{}, entries[1].Value, "an empty value is not nil")
	assert.Nil(t, entries[1].ExpiresAt)

	assert.Equal(t, "entry/text", entries[2].Key)
	assert.Equal(t, []byte("plain text"), entries[2].Value, "string values from MSet read back as bytes")
	assert.Nil(t, entries[2].ExpiresAt, "persistent keys have no expiration")

	for _, entry := range entries {
		assert.Equal(t, time.UTC, entry.UpdatedAt.Location(), entry.Key)
		assert.False(t, entry.UpdatedAt.Before(before), "%s updated at %v, before %v", entry.Key, entry.UpdatedAt, before)
		assert.False(t, entry.UpdatedAt.After(after), "%s updated at %v, after %v", entry.Key, entry.UpdatedAt, after)
	}
}

func TestDelPrefixEmpty(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	seed(t, db, "keep/", "keep/me")
	deleted, err := db.DelPrefix("")
	assert.Error(t, err, "an empty prefix must never delete everything")
	assert.Zero(t, deleted)
	assert.Equal(t, 1, storedCount(t, db, "keep/me"))
}

func TestScanWhileWriterReserved(t *testing.T) {
	db := newTestDatabase(t, testDSN(t))
	if db.Raw() == db.RawReader() && driver == "sqlite" {
		t.Skip("private memory and temporary databases share one connection for reads and writes")
	}
	seed(t, db, "reader/", "reader/key")
	conn, err := db.Raw().Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	type result struct {
		keys []string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		entries, err := db.Scan("reader/", "", 10)
		var keys []string
		for _, entry := range entries {
			keys = append(keys, entry.Key)
		}
		done <- result{keys, err}
	}()
	select {
	case got := <-done:
		require.NoError(t, got.err)
		assert.Equal(t, []string{"reader/key"}, got.keys)
	case <-time.After(2 * time.Second):
		t.Fatal("Scan blocked behind the writer pool")
	}
}
