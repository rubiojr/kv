package main

import (
	"errors"
	"fmt"
	"os"

	"flag"

	"github.com/rubiojr/kv"
)

var driver string

func main() {
	flag.Parse()
	if err := run(); err != nil {
		abort(err.Error())
	}
}

func run() (err error) {
	var db kv.Database
	switch driver {
	case "mysql":
		db, err = useMySQL()
	case "sqlite":
		db, err = useSqlite()
	default:
		return fmt.Errorf("unsupported driver %q", driver)
	}
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := db.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close database: %w", closeErr))
		}
	}()

	var cmd string
	var rem []string
	if flag.NArg() > 0 {
		rem = os.Args[len(os.Args)-flag.NArg():]
		cmd = rem[0]
	}

	switch cmd {
	case "set":
		if len(rem) != 3 {
			return errors.New("invalid number of arguments")
		}

		k := rem[1]
		v := rem[2]
		err = db.Set(k, []byte(v), nil)
		if err != nil {
			return err
		}
	case "get":
		if len(rem) < 2 {
			return errors.New("invalid number of arguments")
		}
		for _, k := range rem[1:] {
			v, err := db.Get(k)
			if err != nil {
				return err
			}
			if _, err := fmt.Println(string(v)); err != nil {
				return err
			}
		}
	default:
		return errors.New("usage: kv [options] get|set key [value1, value2...]")
	}
	return nil
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

// abort prints msg literally to stderr with a trailing newline and exits with status 1.
func abort(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
