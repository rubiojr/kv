package sqlite

import (
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadIterationErrors(t *testing.T) {
	readError := errors.New("row stream interrupted")
	for _, operation := range []struct {
		name      string
		withValue bool
		read      func(*Database) error
	}{
		{"mget", true, func(db *Database) error { _, err := db.MGet("a", "b"); return err }},
		{"mexists", false, func(db *Database) error { _, err := db.MExists("a", "b"); return err }},
		{"get", true, func(db *Database) error { _, err := db.Get("a"); return err }},
		{"exists", false, func(db *Database) error { _, err := db.Exists("a"); return err }},
	} {
		for _, failAt := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/after_%d_rows", operation.name, failAt), func(t *testing.T) {
				raw, mock, err := sqlmock.New()
				require.NoError(t, err)
				t.Cleanup(func() {
					mock.ExpectClose()
					assert.NoError(t, raw.Close())
					assert.NoError(t, mock.ExpectationsWereMet())
				})
				db := &Database{t: "key_values", db: raw}
				rows := sqlmock.NewRows([]string{"key"}).AddRow("a").AddRow("b")
				if operation.withValue {
					rows = sqlmock.NewRows([]string{"key", "value"}).AddRow("a", "A").AddRow("b", "B")
				}
				// Query succeeds; the error arrives only when Next advances the stream.
				rows.RowError(failAt, readError)
				mock.ExpectQuery("SELECT").WillReturnRows(rows).RowsWillBeClosed()

				assert.ErrorIs(t, operation.read(db), readError)
			})
		}
	}
}
