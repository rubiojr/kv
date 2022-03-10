package main

import (
	"fmt"

	"github.com/rubiojr/kv"
)

func main() {
	db, err := kv.New("sqlite", "file://foo")
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
