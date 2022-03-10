package main

import (
	"fmt"

	"github.com/rubiojr/kv"
	"github.com/rubiojr/kv/driver/sqlite"
)

func main() {
	fmt.Println("vim-go")
	d := &sqlite.Driver{}
	err := d.Init(kv.TABLE_NAME, "db.db")
	if err != nil {
		panic(err)
	}
	kv.New(d)
}
