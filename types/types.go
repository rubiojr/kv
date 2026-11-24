package types

import "time"

type KeyValues map[string]interface{}

// Entry is a live key with its value and timestamps, as returned by Scan.
type Entry struct {
	Key   string
	Value []byte
	// ExpiresAt is the absolute expiration timestamp in UTC, or nil for a
	// persistent key.
	ExpiresAt *time.Time
	// UpdatedAt is when the key was last written, in UTC. Its precision is the
	// column's: the MySQL schema in the README stores whole seconds.
	UpdatedAt time.Time
}
