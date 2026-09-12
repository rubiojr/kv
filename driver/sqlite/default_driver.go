package sqlite

import _ "modernc.org/sqlite"

// Keep default-driver registration separate from driver-neutral SQLite setup.
const defaultDriverName = "sqlite"
