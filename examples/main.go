package main

import (
	"fmt"

	"github.com/rubiojr/kv"
	"github.com/vmihailenco/msgpack"
)

var driver string

func main() {
	db, err := kv.New("sqlite", "sqlite.db")

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

	// store a binary blob
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
