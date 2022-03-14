# KV

Work in progress.

A simple key/value store on top of SQLite or MySQL.

Go port of [GitHub's KV](https://github.com/github/github-ds/blob/master/lib/github/kv.rb).

Aims to be 100% compatible with that implementation.

## Initialization 

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

// Get multiple keys
values, err := db.MGet("foo", "staff")
if err != nil {
	panic(err)
}
// iterate the results
for _, v := range values {
	fmt.Println(string(v))
}
```

## TODO

- [ ] Custom driver options for both MySQL and SQLite
- [ ] mset
- [ ] exists
- [ ] mexists
- [ ] setnx
- [ ] increment
- [ ] del
- [ ] mdel
- [ ] ttl
- [ ] mttl
