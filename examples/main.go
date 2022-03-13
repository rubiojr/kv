package main

import (
	"fmt"

	"flag"

	"github.com/rubiojr/kv"
)

var driver string

func main() {
	flag.Parse()

	var db kv.Database
	var err error
	switch driver {
	case "mysql":
		db, err = useMySQL()
	case "sqlite":
		db, err = useSqlite()
	default:
	}

	if err != nil {
		panic(err)
	}
	err = db.Set("foo", []byte("bar"), nil)
	if err != nil {
		panic(err)
	}

	err = db.Set("stuff", []byte("staff"), nil)
	if err != nil {
		panic(err)
	}

	v, err := db.Get("stuff")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))

	// key not found error
	_, err = db.Get("stuff")
	if err != nil {
		fmt.Println(err)
	}

	values, err := db.MGet("foo", "staff")
	if err != nil {
		panic(err)
	}

	for _, v := range values {
		fmt.Println(string(v))
	}
}

func useSqlite() (kv.Database, error) {
	return kv.New("sqlite", "sqlite.db")
}

func useMySQL() (kv.Database, error) {
	return kv.New("mysql", "root:toor@tcp(127.0.0.1:3306)/gokv")
}

func init() {
	flag.StringVar(&driver, "driver", "mysql", "driver to use")
}
