package mysql

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMock returns a store over sqlmock that matches SQL exactly.
func newMock(t *testing.T) (*Database, sqlmock.Sqlmock) {
	t.Helper()
	raw, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		mock.ExpectClose()
		assert.NoError(t, raw.Close())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
	return &Database{t: "key_values", db: raw}, mock
}

// The table's default collation is case-insensitive, so ranges and ordering
// compare the key's bytes, and bounds arrive hex-encoded as binary strings.
func TestScanComparesBytes(t *testing.T) {
	db, mock := newMock(t)
	rows := sqlmock.NewRows([]string{"key", "value", "expires_at", "updated_at"}).
		AddRow("ñ/a", []byte("A"), "2099-01-02 03:04:05", "2026-11-24 12:00:00").
		AddRow("ñ/b", []byte("B"), nil, "2026-11-24 12:00:01")
	mock.ExpectQuery("SELECT `key`, value, CAST(expires_at AS CHAR), CAST(updated_at AS CHAR) FROM key_values"+
		" WHERE CAST(`key` AS BINARY) >= UNHEX(?) AND CAST(`key` AS BINARY) < UNHEX(?) AND (expires_at IS NULL OR expires_at > ?)"+
		" ORDER BY CAST(`key` AS BINARY) LIMIT ?").
		WithArgs("c3b12f", "c3b130", sqlmock.AnyArg(), 2).
		WillReturnRows(rows).RowsWillBeClosed()

	entries, err := db.Scan("ñ/", "", 2)
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "ñ/a", entries[0].Key)
	assert.Equal(t, []byte("A"), entries[0].Value)
	require.NotNil(t, entries[0].ExpiresAt)
	assert.Equal(t, time.Date(2099, 1, 2, 3, 4, 5, 0, time.UTC), *entries[0].ExpiresAt)
	assert.Nil(t, entries[1].ExpiresAt)
	assert.Equal(t, time.Date(2026, 11, 24, 12, 0, 1, 0, time.UTC), entries[1].UpdatedAt)
}

func TestScanAfterComparesBytes(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectQuery("SELECT `key`, value, CAST(expires_at AS CHAR), CAST(updated_at AS CHAR) FROM key_values"+
		" WHERE CAST(`key` AS BINARY) > UNHEX(?) AND (expires_at IS NULL OR expires_at > ?)"+
		" ORDER BY CAST(`key` AS BINARY) LIMIT ?").
		WithArgs("4162", sqlmock.AnyArg(), 10).
		WillReturnRows(sqlmock.NewRows([]string{"key", "value", "expires_at", "updated_at"})).RowsWillBeClosed()

	entries, err := db.Scan("", "Ab", 10)
	require.NoError(t, err)
	assert.NotNil(t, entries, "no matches is an empty slice")
	assert.Empty(t, entries)
}

// A computed bound need not be valid UTF-8; hex keeps it intact.
func TestScanInvalidUTF8Bound(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectQuery("SELECT `key`, value, CAST(expires_at AS CHAR), CAST(updated_at AS CHAR) FROM key_values"+
		" WHERE CAST(`key` AS BINARY) >= UNHEX(?) AND CAST(`key` AS BINARY) < UNHEX(?) AND (expires_at IS NULL OR expires_at > ?)"+
		" ORDER BY CAST(`key` AS BINARY) LIMIT ?").
		WithArgs("c2bf", "c2c0", sqlmock.AnyArg(), 1).
		WillReturnRows(sqlmock.NewRows([]string{"key", "value", "expires_at", "updated_at"})).RowsWillBeClosed()

	_, err := db.Scan("¿", "", 1)
	require.NoError(t, err)
}

func TestDelPrefixComparesBytes(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectExec("DELETE FROM key_values WHERE CAST(`key` AS BINARY) >= UNHEX(?) AND CAST(`key` AS BINARY) < UNHEX(?)").
		WithArgs("41622f", "416230").
		WillReturnResult(sqlmock.NewResult(0, 3))

	deleted, err := db.DelPrefix("Ab/")
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)
}

func TestPrefixArgumentErrors(t *testing.T) {
	db, _ := newMock(t)
	// No expectations: any statement fails the test.
	entries, err := db.Scan("p/", "", 0)
	assert.Error(t, err)
	assert.Nil(t, entries)
	deleted, err := db.DelPrefix("")
	assert.Error(t, err)
	assert.Zero(t, deleted)
}
