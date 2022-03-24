# KV

Work in progress, not ready for prime time.

A simple key/value store on top of SQLite or MySQL (Go port of [GitHub's KV](https://github.com/github/github-ds/blob/master/lib/github/kv.rb)).

Aims to be 100% compatible with that implementation.

## Initialization 

Import the module first:

```Go
import "github.com/rubiojr/kv"
```

### MySQL

```Go
db, err := kv.New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
```

Creates the `gokv` database and a `key_values` table to store key/values.

### SQLite

```Go
db, err := kv.New("sqlite", "my.db")
```

Creates a `key_values` table in `my.db` file database to store key/values.

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

## TODO

- [ ] Custom driver options for both MySQL and SQLite
- [ ] exists
- [ ] mexists
- [ ] setnx
- [ ] increment
- [ ] ttl
- [ ] mttl
