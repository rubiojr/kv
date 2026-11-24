package mysql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	casSelect = "SELECT 1 FROM key_values WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND value = UNHEX(?) FOR UPDATE"
	casUpdate = "UPDATE key_values SET value=?, updated_at=?, expires_at=? WHERE `key`=?"
	cadDelete = "DELETE FROM key_values WHERE `key`=? AND (expires_at IS NULL OR expires_at > ?) AND value = UNHEX(?)"
)

func TestCompareAndSwapLocksMatchingValue(t *testing.T) {
	db, mock := newMock(t)
	expires := time.Date(2099, 1, 2, 3, 4, 5, 0, time.FixedZone("west", -7*3600))
	original := expires
	mock.ExpectBegin()
	mock.ExpectQuery(casSelect).WithArgs("k", sqlmock.AnyArg(), "00ff6f6c64").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectExec(casUpdate).WithArgs([]byte("new"), sqlmock.AnyArg(), expires.UTC(), "k").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	swapped, err := db.CompareAndSwap("k", []byte("\x00\xffold"), []byte("new"), &expires)
	require.NoError(t, err)
	assert.True(t, swapped)
	assert.True(t, original == expires, "CompareAndSwap must not change the caller's timestamp") //nolint:staticcheck // QF1009: compare the full value to detect mutation, including location.
}

// An identical swap may report no changed rows; the locking read decides.
func TestCompareAndSwapUnchangedRow(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(casSelect).WithArgs("k", sqlmock.AnyArg(), "").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	mock.ExpectExec(casUpdate).WithArgs([]byte{}, sqlmock.AnyArg(), nil, "k").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	swapped, err := db.CompareAndSwap("k", nil, []byte{}, nil)
	require.NoError(t, err)
	assert.True(t, swapped)
}

func TestCompareAndSwapMismatch(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectQuery(casSelect).WithArgs("k", sqlmock.AnyArg(), "6f6c64").
		WillReturnRows(sqlmock.NewRows([]string{"1"}))
	mock.ExpectRollback()

	swapped, err := db.CompareAndSwap("k", []byte("old"), []byte("new"), nil)
	require.NoError(t, err)
	assert.False(t, swapped)
}

func TestCompareAndSwapErrors(t *testing.T) {
	failure := errors.New("database failure")
	for _, step := range []string{"begin", "select", "update", "commit"} {
		t.Run(step, func(t *testing.T) {
			db, mock := newMock(t)
			begin := mock.ExpectBegin()
			if step == "begin" {
				begin.WillReturnError(failure)
			} else {
				query := mock.ExpectQuery(casSelect)
				if step == "select" {
					query.WillReturnError(failure)
					mock.ExpectRollback()
				} else {
					query.WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
					exec := mock.ExpectExec(casUpdate)
					if step == "update" {
						exec.WillReturnError(failure)
						mock.ExpectRollback()
					} else {
						exec.WillReturnResult(sqlmock.NewResult(0, 1))
						mock.ExpectCommit().WillReturnError(failure)
					}
				}
			}
			swapped, err := db.CompareAndSwap("k", []byte("old"), []byte("new"), nil)
			assert.ErrorIs(t, err, failure)
			assert.False(t, swapped)
		})
	}
}

func TestCompareAndDeleteComparesBytes(t *testing.T) {
	db, mock := newMock(t)
	mock.ExpectBegin()
	mock.ExpectExec(cadDelete).WithArgs("k", sqlmock.AnyArg(), "4f6c64").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectExec(cadDelete).WithArgs("k", sqlmock.AnyArg(), "").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()

	deleted, err := db.CompareAndDelete("k", []byte("Old"))
	require.NoError(t, err)
	assert.True(t, deleted)
	deleted, err = db.CompareAndDelete("k", nil)
	require.NoError(t, err)
	assert.False(t, deleted, "nothing deleted is a mismatch")
}

func TestCompareAndDeleteErrors(t *testing.T) {
	failure := errors.New("database failure")
	for _, step := range []string{"begin", "delete", "rows", "commit"} {
		t.Run(step, func(t *testing.T) {
			db, mock := newMock(t)
			begin := mock.ExpectBegin()
			switch step {
			case "begin":
				begin.WillReturnError(failure)
			case "delete":
				mock.ExpectExec(cadDelete).WillReturnError(failure)
				mock.ExpectRollback()
			case "rows":
				mock.ExpectExec(cadDelete).WillReturnResult(sqlmock.NewErrorResult(failure))
				mock.ExpectRollback()
			case "commit":
				mock.ExpectExec(cadDelete).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectCommit().WillReturnError(failure)
			}
			deleted, err := db.CompareAndDelete("k", []byte("old"))
			assert.ErrorIs(t, err, failure)
			assert.False(t, deleted, "a failed delete must not report success")
		})
	}
}

func TestCompareDeadlockRetries(t *testing.T) {
	deadlock := &mysqldriver.MySQLError{Number: 1213, Message: "deadlock"}
	for _, operation := range []string{"swap", "delete"} {
		for _, failures := range []int{1, 3} {
			t.Run(fmt.Sprintf("%s/%d_deadlocks", operation, failures), func(t *testing.T) {
				db, mock := newMock(t)
				for range failures {
					mock.ExpectBegin()
					if operation == "swap" {
						mock.ExpectQuery(casSelect).WillReturnError(deadlock)
					} else {
						mock.ExpectExec(cadDelete).WillReturnError(deadlock)
					}
					mock.ExpectRollback()
				}
				if failures < 3 {
					mock.ExpectBegin()
					if operation == "swap" {
						mock.ExpectQuery(casSelect).WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
						mock.ExpectExec(casUpdate).WillReturnResult(sqlmock.NewResult(0, 1))
					} else {
						mock.ExpectExec(cadDelete).WillReturnResult(sqlmock.NewResult(0, 1))
					}
					mock.ExpectCommit()
				}
				var done bool
				var err error
				if operation == "swap" {
					done, err = db.CompareAndSwap("k", []byte("old"), []byte("new"), nil)
				} else {
					done, err = db.CompareAndDelete("k", []byte("old"))
				}
				if failures < 3 {
					require.NoError(t, err, "a deadlock victim retries")
					assert.True(t, done)
				} else {
					assert.ErrorIs(t, err, deadlock, "retries are bounded")
					assert.False(t, done)
				}
			})
		}
	}
}
