# Contributing to KV

## Requirements

* docker and docker-compose to run tests
* Modern Go version
* Netcat (nc) command available in PATH

## Running the test suite

It's recommended to run the the test suite before sending patches:

```
./script/ci
```

SQLite tests use per-test temporary databases and remove them after closing all
connections. Set `GOKV_DRIVER` and `GOKV_DSN` to test an explicitly configured
database; databases supplied through `GOKV_DSN` are not deleted by the tests.
