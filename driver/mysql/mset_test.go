package mysql

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rubiojr/kv/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMSetEmpty(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	for _, tc := range []struct {
		name   string
		values types.KeyValues
		expiry *time.Time
	}{
		{"nil", nil, nil},
		{"empty", types.KeyValues{}, nil},
		{"nil_with_expiry", nil, &past},
		{"empty_with_expiry", types.KeyValues{}, &past},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() {
				mock.ExpectClose()
				assert.NoError(t, raw.Close())
				assert.NoError(t, mock.ExpectationsWereMet())
			})
			db := &Database{t: "key_values", db: raw}

			// Empty batches should succeed without executing any SQL.
			assert.NoError(t, db.MSet(tc.values, tc.expiry))
		})
	}
}
