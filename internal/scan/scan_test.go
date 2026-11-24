package scan

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var plain = Dialect{Key: "`key`", Bound: "?", Arg: func(bound string) any { return bound }, Text: "TEXT"}

func TestSuccessor(t *testing.T) {
	for _, tc := range []struct {
		prefix, want string
		bounded      bool
	}{
		{"", "", false},
		{"a", "b", true},
		{"ab", "ac", true},
		{"a%", "a&", true},
		{"a_", "a`", true},
		{"a\x00", "a\x01", true},
		{"a\xff", "b", true},
		{"a\xff\xff", "b", true},
		{"\xff", "", false},
		{"\xff\xff", "", false},
		{"ñ", "\xc3\xb2", true},              // ñ is C3 B1; ò is C3 B2
		{"¿", "\xc2\xc0", true},              // C2 BF: the bound is not valid UTF-8
		{"日", "\xe6\x97\xa6", true},          // E6 97 A5 -> E6 97 A6, which is 旦
		{"\x7f", "\x80", true},               // the bound leaves ASCII
		{"z\xff\xfe\xff", "z\xff\xff", true}, // only trailing 0xFF bytes are dropped
	} {
		got, bounded := Successor(tc.prefix)
		assert.Equal(t, tc.bounded, bounded, "prefix %q", tc.prefix)
		assert.Equal(t, tc.want, got, "prefix %q", tc.prefix)
		if !bounded {
			continue
		}
		// Every extension of prefix sorts before the bound, and the bound itself
		// does not start with prefix.
		for _, suffix := range []string{"", "\x00", "a", "\xff", "\xff\xff\xff", "ñ"} {
			assert.Less(t, tc.prefix+suffix, got, "prefix %q suffix %q", tc.prefix, suffix)
		}
		assert.False(t, strings.HasPrefix(got, tc.prefix), "prefix %q", tc.prefix)
	}
}

func TestKeyRange(t *testing.T) {
	for _, tc := range []struct {
		name, prefix, after string
		conditions          []string
		args                []any
	}{
		{"all", "", "", []string{"`key` >= ?"}, []any{""}},
		{"all_after", "", "k", []string{"`key` > ?"}, []any{"k"}},
		{"prefix", "p/", "", []string{"`key` >= ?", "`key` < ?"}, []any{"p/", "p0"}},
		{"after_inside", "p/", "p/a", []string{"`key` > ?", "`key` < ?"}, []any{"p/a", "p0"}},
		{"after_equal", "p/", "p/", []string{"`key` > ?", "`key` < ?"}, []any{"p/", "p0"}},
		{"after_before", "p/", "a", []string{"`key` >= ?", "`key` < ?"}, []any{"p/", "p0"}},
		{"unbounded", "\xff", "", []string{"`key` >= ?"}, []any{"\xff"}},
		{"unbounded_after", "\xff", "\xff\x01", []string{"`key` > ?"}, []any{"\xff\x01"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conditions, args := plain.keyRange(tc.prefix, tc.after)
			assert.Equal(t, tc.conditions, conditions)
			assert.Equal(t, tc.args, args)
		})
	}
}

func TestQueries(t *testing.T) {
	now := time.Date(2099, 1, 2, 3, 4, 5, 0, time.UTC)
	hexed := Dialect{Key: "CAST(`key` AS BINARY)", Bound: "UNHEX(?)", Arg: func(b string) any { return "<" + b + ">" }, Text: "CHAR"}

	query, args, err := hexed.ScanQuery("t", "p/", "p/a", 5, now)
	require.NoError(t, err)
	assert.Equal(t, "SELECT `key`, value, CAST(expires_at AS CHAR), CAST(updated_at AS CHAR) FROM t WHERE CAST(`key` AS BINARY) > UNHEX(?) AND CAST(`key` AS BINARY) < UNHEX(?) AND (expires_at IS NULL OR expires_at > ?) ORDER BY CAST(`key` AS BINARY) LIMIT ?", query)
	assert.Equal(t, []any{"<p/a>", "<p0>", now, 5}, args)

	query, args, err = hexed.ScanQuery("t", "", "", 1, now)
	require.NoError(t, err)
	assert.Equal(t, "SELECT `key`, value, CAST(expires_at AS CHAR), CAST(updated_at AS CHAR) FROM t WHERE CAST(`key` AS BINARY) >= UNHEX(?) AND (expires_at IS NULL OR expires_at > ?) ORDER BY CAST(`key` AS BINARY) LIMIT ?", query)
	assert.Equal(t, []any{"<>", now, 1}, args, "an empty prefix keeps an empty lower bound")

	for _, limit := range []int{0, -1} {
		_, _, err := hexed.ScanQuery("t", "p/", "", limit, now)
		assert.ErrorIs(t, err, errLimit)
	}

	query, args, err = hexed.DelPrefixQuery("t", "p/")
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM t WHERE CAST(`key` AS BINARY) >= UNHEX(?) AND CAST(`key` AS BINARY) < UNHEX(?)", query)
	assert.Equal(t, []any{"<p/>", "<p0>"}, args)

	query, args, err = hexed.DelPrefixQuery("t", "\xff")
	require.NoError(t, err)
	assert.Equal(t, "DELETE FROM t WHERE CAST(`key` AS BINARY) >= UNHEX(?)", query, "a 0xFF prefix has no upper bound")
	assert.Equal(t, []any{"<\xff>"}, args)

	_, _, err = hexed.DelPrefixQuery("t", "")
	assert.ErrorIs(t, err, errEmptyPrefix)
}

func TestScanValidatesBeforeQuerying(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		assert.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
	// No expectations: any statement fails the test.
	entries, err := plain.Scan(db, "t", "p/", "", 0)
	assert.ErrorIs(t, err, errLimit)
	assert.Nil(t, entries)
	deleted, err := plain.DelPrefix(db, "t", "")
	assert.ErrorIs(t, err, errEmptyPrefix)
	assert.Zero(t, deleted)
}

func TestEntries(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		assert.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
	rows := sqlmock.NewRows([]string{"key", "value", "expires_at", "updated_at"}).
		AddRow("a", []byte{0, 1}, "2099-01-02T10:04:05+07:00", "2026-11-24 12:00:00").
		AddRow("b", nil, nil, "2026-11-24 12:00:01.5 +0000 UTC")
	mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()

	got, err := plain.Scan(db, "t", "", "", 10)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, "a", got[0].Key)
	assert.Equal(t, []byte{0, 1}, got[0].Value)
	require.NotNil(t, got[0].ExpiresAt)
	assert.Equal(t, time.Date(2099, 1, 2, 3, 4, 5, 0, time.UTC), *got[0].ExpiresAt, "expirations are converted to UTC")
	assert.Equal(t, time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC), got[0].UpdatedAt)
	assert.Equal(t, "b", got[1].Key)
	assert.Equal(t, []byte{}, got[1].Value, "a nil value decodes as empty")
	assert.Nil(t, got[1].ExpiresAt)
	assert.Equal(t, time.Date(2026, 11, 24, 12, 0, 1, 500000000, time.UTC), got[1].UpdatedAt)
	assert.Equal(t, time.UTC, got[1].UpdatedAt.Location())
}

func TestEntriesErrors(t *testing.T) {
	want := errors.New("database failure")
	for _, failure := range []string{"query", "scan", "expires_at", "updated_at", "iteration"} {
		t.Run(failure, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				mock.ExpectClose()
				assert.NoError(t, db.Close())
				assert.NoError(t, mock.ExpectationsWereMet())
			})
			query := mock.ExpectQuery("SELECT")
			rows := sqlmock.NewRows([]string{"key", "value", "expires_at", "updated_at"})
			switch failure {
			case "query":
				query.WillReturnError(want)
			case "scan":
				query.WillReturnRows(rows.AddRow(nil, "v", nil, "2026-11-24 12:00:00")).RowsWillBeClosed()
			case "expires_at":
				query.WillReturnRows(rows.AddRow("a", "v", "soon", "2026-11-24 12:00:00")).RowsWillBeClosed()
			case "updated_at":
				query.WillReturnRows(rows.AddRow("a", "v", nil, "earlier")).RowsWillBeClosed()
			case "iteration":
				rows.AddRow("a", "v", nil, "2026-11-24 12:00:00").AddRow("b", "v", nil, "2026-11-24 12:00:00").RowError(1, want)
				query.WillReturnRows(rows).RowsWillBeClosed()
			}
			got, err := plain.Scan(db, "t", "", "", 10)
			require.Error(t, err)
			assert.Nil(t, got, "failed reads must not return successful-looking partial data")
			if failure == "query" || failure == "iteration" {
				assert.ErrorIs(t, err, want)
			}
		})
	}
}

func TestDelPrefixErrors(t *testing.T) {
	want := errors.New("database failure")
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		assert.NoError(t, db.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
	mock.ExpectExec("DELETE").WithArgs("p/", "p0").WillReturnError(want)
	mock.ExpectExec("DELETE").WithArgs("p/", "p0").WillReturnResult(sqlmock.NewErrorResult(want))
	mock.ExpectExec("DELETE").WithArgs("p/", "p0").WillReturnResult(sqlmock.NewResult(0, 7))
	for _, wantCount := range []int64{0, 0} {
		got, err := plain.DelPrefix(db, "t", "p/")
		assert.ErrorIs(t, err, want)
		assert.Equal(t, wantCount, got)
	}
	got, err := plain.DelPrefix(db, "t", "p/")
	require.NoError(t, err)
	assert.Equal(t, int64(7), got)
}
