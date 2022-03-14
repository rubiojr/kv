package main

import (
	"fmt"
	"os"

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
		abort(err.Error())
	}

	var cmd string
	var rem []string
	if flag.NArg() > 0 {
		rem = os.Args[len(os.Args)-flag.NArg():]
		cmd = rem[0]
	}

	switch cmd {
	case "set":
		if len(rem) != 3 {
			abort("invalid number of arguments")
		}

		k := rem[1]
		v := rem[2]
		err = db.Set(k, []byte(v), nil)
		if err != nil {
			abort(err.Error())
		}
	case "get":
		if len(rem) < 2 {
			abort("invalid number of arguments")
		}
		for _, k := range rem[1:] {
			v, err := db.Get(k)
			if err != nil {
				abort(err.Error())
			}
			fmt.Println(string(v))
		}
	default:
		abort("Usage: kv [options] get|set key [value1, value2...]")
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

func abort(msg string) {
	fmt.Fprintf(os.Stderr, msg+"\n")
	os.Exit(1)
}
