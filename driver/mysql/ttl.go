package mysql

import (
	"time"

	"github.com/rubiojr/kv/internal/expiry"
)

// TTL returns the expiration timestamp, or nil for missing, expired, or persistent keys.
func (d *Database) TTL(key string) (*time.Time, error) {
	values, err := d.MTTL(key)
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

// MTTL returns expiration timestamps in input order, including duplicates.
// Missing, expired, and persistent keys have nil entries. Empty input is a no-op.
func (d *Database) MTTL(keys ...string) ([]*time.Time, error) {
	return expiry.Read(d.RawReader(), d.t, "CHAR", keys...)
}
