package mysql

import (
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetNXDuplicateErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		live bool
	}{
		{"live_key", true},
		{"other_unique_constraint", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				mock.ExpectClose()
				assert.NoError(t, raw.Close())
				assert.NoError(t, mock.ExpectationsWereMet())
			})
			duplicate := &mysqldriver.MySQLError{Number: 1062, Message: "duplicate entry"}
			mock.ExpectBegin()
			mock.ExpectQuery("SELECT expires_at.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"expired"}))
			mock.ExpectExec("INSERT INTO").WillReturnError(duplicate)
			rows := sqlmock.NewRows([]string{"expired"})
			if tc.live {
				rows.AddRow(false)
			}
			mock.ExpectQuery("SELECT expires_at").WillReturnRows(rows)
			mock.ExpectRollback()
			db := &Database{db: raw, t: "key_values"}
			stored, err := db.SetNX("key", []byte("value"), nil)
			assert.False(t, stored)
			if tc.live {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, duplicate)
			}
		})
	}
}

func TestSetNXDeadlockRetries(t *testing.T) {
	for _, failures := range []int{1, 3} {
		t.Run(map[int]string{1: "recovers", 3: "bounded"}[failures], func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				mock.ExpectClose()
				assert.NoError(t, raw.Close())
				assert.NoError(t, mock.ExpectationsWereMet())
			})
			deadlock := &mysqldriver.MySQLError{Number: 1213, Message: "deadlock"}
			for range failures {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT expires_at.*FOR UPDATE").WillReturnError(deadlock)
				mock.ExpectRollback()
			}
			if failures == 1 {
				mock.ExpectBegin()
				mock.ExpectQuery("SELECT expires_at.*FOR UPDATE").WillReturnRows(sqlmock.NewRows([]string{"expired"}))
				mock.ExpectExec("INSERT INTO").WillReturnResult(sqlmock.NewResult(1, 1))
				mock.ExpectCommit()
			}
			db := &Database{db: raw, t: "key_values"}
			stored, err := db.SetNX("key", []byte("value"), nil)
			if failures == 1 {
				require.NoError(t, err)
				assert.True(t, stored)
			} else {
				assert.ErrorIs(t, err, deadlock)
				assert.False(t, stored)
			}
		})
	}
}
