package sqlite

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

type Driver struct {
	db *sql.DB
}

func (d *Driver) Init(tableName, urn string) error {
	db, err := sql.Open("sqlite", urn)
	if err != nil {
		return err
	}

	sql := fmt.Sprintf(`
DROP TABLE IF EXISTS '%s';
CREATE TABLE %s(
  'id' bigint(20) NOT NULL,
  'key' varchar(255) NOT NULL,
  'value' blob NOT NULL,
  'created_at' datetime NOT NULL,
  'updated_at' datetime NOT NULL,
  'expires_at' datetime DEFAULT NULL,
  PRIMARY KEY ('id')
);
`, tableName, tableName)

	_, err = db.Exec(sql)
	d.db = db

	return err
}

func (d *Driver) Get(key string) ([]byte, error) {
	return nil, nil
}
