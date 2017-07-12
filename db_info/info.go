package db_info

import (
	"log"

	"github.com/jmoiron/sqlx"
)

type DBInfo struct {
	dsn          string
	conn         *sqlx.DB
	hasTableLock struct {
		checked bool
		yes     bool
	}
}

func New(dsn string) (*DBInfo, error) {
	i := &DBInfo{
		dsn: dsn,
	}
	return i, i.Ping()
}

func (i *DBInfo) Ping() error {
	var err error
	i.conn, err = sqlx.Connect("mysql", i.dsn)
	if err != nil {
		return err
	}
	return i.conn.Ping()
}

func (i *DBInfo) Tables() []string {
	result, err := i.conn.Query("SHOW FULL TABLES WHERE Table_type LIKE 'BASE TABLE'")
	if err != nil {
		log.Fatalf("Error listing tables: %s", err)
	}
	defer result.Close()
	var tables []string
	for result.Next() {
		var table, kind string
		if err := result.Scan(&table, &kind); err != nil {
			log.Fatalf("Error scanning: %s", err)
		}
		tables = append(tables, table)
	}
	return tables
}

func (i *DBInfo) HasBackupLock() bool {
	if i.hasTableLock.checked {
		return i.hasTableLock.yes
	}
	result, err := i.conn.Query("SELECT @@have_backup_locks")
	if err != nil {
		return false
	}
	defer result.Close()
	if result.Next() {
		var what string
		if err := result.Scan(&what); err != nil {
			log.Printf("Error scanning: %s", err)
		}
		i.hasTableLock.yes = what == "YES"
	}
	return i.hasTableLock.yes
}
