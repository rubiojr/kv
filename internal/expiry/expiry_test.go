package expiry

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimestampFormats(t *testing.T) {
	want := time.Date(2099, 1, 2, 3, 4, 5, 123456000, time.UTC)
	for _, value := range []string{
		"2099-01-02T03:04:05.123456Z",
		"2099-01-02 03:04:05.123456 +0000 UTC",
		"2099-01-02 03:04:05.123456+00:00",
		"2099-01-02 03:04:05.123456",
		"2099-01-02T10:04:05.123456+07:00",
	} {
		t.Run(value, func(t *testing.T) {
			got, err := ParseTimestamp(value)
			require.NoError(t, err)
			assert.True(t, want.Equal(got))
		})
	}
}

func TestReadErrors(t *testing.T) {
	for _, failure := range []string{"query", "scan", "timestamp", "iteration"} {
		t.Run(failure, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				mock.ExpectClose()
				assert.NoError(t, db.Close())
				assert.NoError(t, mock.ExpectationsWereMet())
			})
			want := errors.New("database failure")
			query := mock.ExpectQuery("SELECT")
			rows := sqlmock.NewRows([]string{"key", "expires_at"})
			switch failure {
			case "query":
				query.WillReturnError(want)
			case "scan":
				query.WillReturnRows(rows.AddRow(nil, nil)).RowsWillBeClosed()
			case "timestamp":
				query.WillReturnRows(rows.AddRow("a", "not a timestamp")).RowsWillBeClosed()
			case "iteration":
				rows.AddRow("a", "2099-01-02T03:04:05Z").AddRow("b", nil).RowError(1, want)
				query.WillReturnRows(rows).RowsWillBeClosed()
			}
			got, err := Read(db, "key_values", "TEXT", "a", "b")
			require.Error(t, err)
			assert.Nil(t, got, "failed reads must not return successful-looking partial data")
			if failure == "query" || failure == "iteration" {
				assert.ErrorIs(t, err, want)
			}
		})
	}
}
