package main

import (
	"fmt"

	"github.com/rubiojr/kv"
	"github.com/rubiojr/kv/driver/sqlite"
)

func main() {
	d := &sqlite.Driver{}
	err := d.Init(kv.TABLE_NAME, "db.db")
	if err != nil {
		panic(err)
	}

	kv := kv.New(d)
	err = kv.Set("foo", []byte("bar"), nil)
	if err != nil {
		panic(err)
	}

	err = kv.Set("stuff", []byte("staff"), nil)
	if err != nil {
		panic(err)
	}

	v, err := kv.Get("stuff")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(v))

	// key not found error
	_, err = kv.Get("stuff")
	if err != nil {
		fmt.Println(err)
	}

	values, err := kv.MGet("foo", "staff")
	if err != nil {
		panic(err)
	}

	for _, v := range values {
		fmt.Println(string(v))
	}
}
