# KV

A simple key/value store on top of SQLite or MySQL (Go port of [GitHub's KV](https://github.com/github/github-ds/blob/master/lib/github/kv.rb)).

Aims compatible with the original implementation by default, offering a few extra backend drivers and some extra configuration knobs.

## Initialization 

Import the module first:

```Go
import "github.com/rubiojr/kv"
```

### MySQL

```Go
db, err := kv.New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
```

Note that the database `gokv` and the table name used by default `key_values` will need to be created before using this driver.

```sql
USE gokv
CREATE TABLE IF NOT EXISTS key_values (
			`id` bigint(20) NOT NULL AUTO_INCREMENT,
			`key` varchar(255) NOT NULL,
			`value` blob NOT NULL,
			`created_at` datetime NOT NULL,
			`updated_at` datetime NOT NULL,
			`expires_at` datetime DEFAULT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY index_key_values_on_key (`key`),
			KEY index_key_values_on_expires_at (expires_at)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8
```

### SQLite

```Go
db, err := kv.New("sqlite", "my.db")
```

Creates a `key_values` table in `my.db` using modernc by default.

File-backed SQLite stores require WAL mode. Use `db.Close()` to close the entire store.
See [SQLite BYOD](docs/sqlite-byod.md) for custom drivers, connection pools,
timeouts, and memory-database behavior.

## Getting and setting keys

Single keys:

```Go
// set a couple of keys
err = db.Set("foo", []byte("bar"), nil)
if err != nil {
	panic(err)
}
err = db.Set("stuff", []byte("staff"), nil)
if err != nil {
	panic(err)
}

// Get one key
v, err := db.Get("foo")
if err != nil {
	panic(err)
}
fmt.Println(string(v)) // prints bar

// Deleting a single key
err := db.Del("foo")
if err != nil {
	panic(err)
}
```

Multiple keys:

```Go
// Get multiple keys
values, err := db.MGet("foo", "staff")
if err != nil {
	panic(err)
}
// iterate the results
for _, v := range values {
	fmt.Println(string(v))
}

// Set multiple keys
values := types.KeyValues{}
values["mset1"] = "msetv1"
values["mset2"] = "msetv2"
err = db.MSet(values, nil)

// Deleting multiple keys
err := db.MDel("mset1", "mset2")
if err != nil {
	panic(err)
}

```

### Set a key only if absent

`SetNX` atomically stores a value when its key is missing or expired. It returns
`true` when stored, or `false` when a live key already exists. Existing values and
expiration timestamps are preserved on a rejected write. A nil expiration makes
the new value persistent. Database and constraint failures return an error.

```Go
stored, err := db.SetNX("claim", []byte("owner"), nil)
if err != nil {
    panic(err)
}
if stored {
    fmt.Println("claim acquired")
}
```

### Expiration timestamps

`TTL` returns a key's expiration timestamp as `*time.Time`, matching GitHub KV.
Missing, expired, and non-expiring keys return `nil` without an error.
`MTTL` returns one entry per requested key, preserving order and duplicates.
An empty `MTTL` call returns an empty slice. Database and decoding failures return
an error.

```Go
expiresAt, err := db.TTL("foo")
if err != nil {
    panic(err)
}
if expiresAt != nil {
    fmt.Println("expires at", *expiresAt)
}

expirations, err := db.MTTL("foo", "missing", "foo")
```

### Storing binary values

An example using [vmihailenco/msgpack](https://github.com/vmihailenco/msgpack) to serialize data.

```Go
// store a string as a binary blob
b, err := msgpack.Marshal("blob")
if err != nil {
	panic(err)
}

err = db.Set("bin", b, nil)
if err != nil {
	panic(err)
}

v, err = db.Get("bin")
if err != nil {
	fmt.Println(err)
}

var blobStr string
err = msgpack.Unmarshal(b, &blobStr)
if err != nil {
	panic(err)
}
fmt.Println(blobStr)
```
