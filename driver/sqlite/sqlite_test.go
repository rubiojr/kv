package sqlite_test

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rubiojr/kv/driver/sqlite"
	"github.com/rubiojr/kv/errors"
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
			db := &sqlite.Database{}
			require.NoError(t, db.Init("key_values", filepath.Join(t.TempDir(), "kv.db")))
			t.Cleanup(func() { require.NoError(t, db.Raw().Close()) })
			require.NoError(t, db.Set("existing", []byte("value"), nil))

			assert.NoError(t, db.MSet(tc.values, tc.expiry))

			value, err := db.Get("existing")
			require.NoError(t, err, "an empty batch must not expire or remove existing keys")
			assert.Equal(t, []byte("value"), value)
		})
	}
}

func TestExpiryTimeZones(t *testing.T) {
	now := time.Now().UTC()
	for _, zone := range []struct {
		name   string
		offset int
	}{
		{"utc", 0},
		{"west", -7 * 60 * 60},
		{"east", 7 * 60 * 60},
	} {
		t.Run(zone.name, func(t *testing.T) {
			db := &sqlite.Database{}
			require.NoError(t, db.Init("key_values", filepath.Join(t.TempDir(), "kv.db")))
			t.Cleanup(func() { require.NoError(t, db.Raw().Close()) })

			location := time.FixedZone(zone.name, zone.offset)
			future := now.Add(time.Hour).In(location)
			past := now.Add(-time.Hour).In(location)
			for _, phase := range []struct {
				name    string
				expiry  *time.Time
				visible bool
			}{
				{"future", &future, true},
				{"expired", &past, false},
				{"revived", &future, true},
				{"expired_again", &past, false},
				{"cleared", nil, true},
			} {
				t.Run(phase.name, func(t *testing.T) {
					var original time.Time
					if phase.expiry != nil {
						original = *phase.expiry
					}
					require.NoError(t, db.Set("single", []byte("value"), phase.expiry))
					require.NoError(t, db.MSet(types.KeyValues{"batch-a": "value", "batch-b": "value"}, phase.expiry))
					if phase.expiry != nil {
						assert.True(t, original == *phase.expiry, "writes must not change the caller's timestamp or location")
					}

					keys := []string{"single", "batch-a", "batch-b"}
					for _, key := range keys {
						value, err := db.Get(key)
						if phase.visible {
							assert.NoError(t, err)
							assert.Equal(t, []byte("value"), value)
						} else {
							assert.ErrorIs(t, err, errors.ErrKeyNotFound)
						}
						exists, err := db.Exists(key)
						require.NoError(t, err)
						assert.Equal(t, phase.visible, exists)
					}

					values, err := db.MGet(keys...)
					require.NoError(t, err)
					if phase.visible {
						assert.Equal(t, [][]byte{[]byte("value"), []byte("value"), []byte("value")}, values)
					} else {
						assert.Empty(t, values)
					}
					exists, err := db.MExists(keys...)
					require.NoError(t, err)
					assert.Equal(t, []bool{phase.visible, phase.visible, phase.visible}, exists)
				})
			}
		})
	}
}
