package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
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
	flag.StringVar(&dir, "dir", "", "Destination directory path")
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
	log.Fatalf("Will use dsn %q", dsn)
}
