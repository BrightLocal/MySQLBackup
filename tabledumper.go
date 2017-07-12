package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/BrightLocal/MySQLBackup/dir_dumper"
	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/worker_pool"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func main() {
	var (
		hostname   string
		port       int
		database   string
		username   string
		password   string
		skipTables string
		dir        string
		streams    int
		dsn        string
	)
	flag.StringVar(&hostname, "hostname", "locahost", "Host name")
	flag.IntVar(&port, "port", 3306, "Port number")
	flag.StringVar(&database, "database", "", "Database name to dump")
	flag.StringVar(&username, "username", "", "User name")
	flag.StringVar(&password, "password", "", "Password")
	flag.StringVar(&skipTables, "skip-tables", "", "Table names to skip")
	flag.StringVar(&dir, "dir", ".", "Destination directory path")
	flag.IntVar(&streams, "streams", 8, "How many tables to dump in parallel")
	flag.Parse()
	if username == "" {
		var err error
		dsn, err = mylogin_reader.Read().GetDSN()
		if err != nil {
			log.Fatalf("Error finding MySQL credentials: %s", err)
		}
	} else {
		dsn = fmt.Sprintf(
			"%s:%s@tcp(%s:%d)",
			username,
			password,
			hostname,
			port,
		)
	}
	dsn += "/" + database
	dd := dir_dumper.NewDirDumper(dsn, dir)
	wp := worker_pool.NewPool(streams, dd.Dump)
	names := make(chan interface{})
	go func() {
		for _, tableName := range getTablesList(dsn) {
			names <- tableName
		}
		close(names)
	}()
	wp.Run(names)
}

func getTablesList(dsn string) []string {
	conn, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		log.Fatalf("Error connecting: %s", err)
	}
	result, err := conn.Query("SHOW TABLES")
	if err != nil {
		log.Fatalf("Error listing tables: %s", err)
	}
	defer result.Close()
	var tables []string
	for result.Next() {
		var table string
		if err := result.Scan(&table); err != nil {
			log.Fatalf("Error scanning: %s", err)
		}
		tables = append(tables, table)
	}
	return tables
}
