package db_info

import (
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

type DBInfo struct {
	dsn          string
	conn         *sqlx.DB
	hasTableLock struct {
		checked bool
		yes     bool
	}
	tableColumnTypes map[string][]string
	masterStatus     struct {
		File            string `db:"File"`
		Position        int    `db:"Position"`
		DoDB            string `db:"Binlog_Do_DB"`
		IgnoreDB        string `db:"Binlog_Ignore_DB"`
		ExecutedGtidSet string `db:"Executed_Gtid_Set"`
	}
	isMaster bool
}

func New(dsn string) (*DBInfo, error) {
	i := &DBInfo{
		dsn:              dsn,
		tableColumnTypes: make(map[string][]string),
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
		i.tableColumnTypes[table] = i.tableColumns(table)
	}
	return tables
}

func (i *DBInfo) getMasterStatus() {
	if result, err := i.conn.Queryx("SHOW MASTER STATUS"); err != nil {
		log.Printf("Could not get master status: %s", err)
	} else {
		defer result.Close()
		for result.Next() {
			if err := result.StructScan(&i.masterStatus); err != nil {
				log.Printf("Error scanning master status: %s", err)
			}
			i.isMaster = true
			break
		}
	}
}

func (i *DBInfo) TableColumnType(tableName string, col int) string {
	if table, ok := i.tableColumnTypes[tableName]; ok {
		if col < len(table) {
			return table[col]
		}
		log.Printf("There's no column %d in table %q", col, tableName)
		return ""
	}
	log.Printf("There's no such table %q", tableName)
	return ""
}

func (i *DBInfo) tableColumns(tableName string) []string {
	result, err := i.conn.Queryx("SHOW COLUMNS FROM " + tableName)
	if err != nil {
		log.Fatalf("Error getting table %q columns: %s", tableName, err)
	}
	defer result.Close()
	var cTypes []string
	for result.Next() {
		var (
			fields []interface{}
			err    error
		)
		if fields, err = result.SliceScan(); err != nil {
			log.Fatalf("Error scanning: %s", err)
		}
		kind := string(fields[1].([]uint8))
		switch {
		// TODO bit type support?
		// String ///
		case strings.HasPrefix(kind, "varchar"):
			fallthrough
		case strings.HasPrefix(kind, "char"):
			fallthrough
		case strings.HasPrefix(kind, "text"):
			fallthrough
		case strings.HasPrefix(kind, "tinytext"):
			fallthrough
		case strings.HasPrefix(kind, "mediumtext"):
			fallthrough
		case strings.HasPrefix(kind, "longtext"):
			fallthrough
		case strings.HasPrefix(kind, "set"):
			fallthrough
		case strings.HasPrefix(kind, "enum"):
			fallthrough
		case strings.HasPrefix(kind, "date"):
			fallthrough
		case strings.HasPrefix(kind, "text"):
			cTypes = append(cTypes, "string")
		// Numeric ///
		case strings.HasPrefix(kind, "int"):
			fallthrough
		case strings.HasPrefix(kind, "smallint"):
			fallthrough
		case strings.HasPrefix(kind, "tinyint"):
			fallthrough
		case strings.HasPrefix(kind, "mediumint"):
			fallthrough
		case strings.HasPrefix(kind, "bigint"):
			fallthrough
		case strings.HasPrefix(kind, "decimal"):
			fallthrough
		case strings.HasPrefix(kind, "numeric"):
			fallthrough
		case strings.HasPrefix(kind, "timestamp"):
			fallthrough
		case strings.HasPrefix(kind, "float"):
			fallthrough
		case strings.HasPrefix(kind, "double"):
			cTypes = append(cTypes, "numeric")
		// Binary ///
		case strings.HasPrefix(kind, "binary"):
			fallthrough
		case strings.HasPrefix(kind, "varbinary"):
			fallthrough
		case strings.HasPrefix(kind, "blob"):
			fallthrough
		case strings.HasPrefix(kind, "tinyblob"):
			fallthrough
		case strings.HasPrefix(kind, "mediumblob"):
			fallthrough
		case strings.HasPrefix(kind, "longblob"):
			cTypes = append(cTypes, "binary")
		default:
			log.Fatalf("Unsupported type %q", kind)
		}
	}
	return cTypes
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
