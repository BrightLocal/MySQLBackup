package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/BrightLocal/MySQLBackup/db_info"
	"github.com/BrightLocal/MySQLBackup/dir_dumper"
	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/worker_pool"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var (
		hostname   string
		port       int
		database   string
		login      string
		username   string
		password   string
		skipTables string
		dir        string
		streams    int
		dsn        string
	)
	flag.StringVar(&hostname, "hostname", "localhost", "Host name")
	flag.IntVar(&port, "port", 3306, "Port number")
	flag.StringVar(&database, "database", "", "Database name to dump")
	flag.StringVar(&login, "login-path", "", "Login path")
	flag.StringVar(&username, "username", "", "User name")
	flag.StringVar(&password, "password", "", "Password")
	flag.StringVar(&skipTables, "skip-tables", "", "Table names to skip")
	flag.StringVar(&dir, "dir", ".", "Destination directory path")
	flag.IntVar(&streams, "streams", 8, "How many tables to dump in parallel")
	flag.Parse()
	if login != "" {
		var err error
		dsn, err = mylogin_reader.Read().GetDSN(login)
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
	if database == "" {
		flag.Usage()
		return
	}
	dsn += database
	skipList := make(map[string]struct{})
	if skipTables != "" {
		for _, t := range strings.Split(skipTables, ",") {
			skipList[strings.TrimSpace(t)] = struct{}{}
		}
	}
	dbInfo, err := db_info.New(dsn)
	if err != nil {
		log.Fatalf("Error connecting to %q: %s", dsn, err)
	}
	if dbInfo.HasBackupLock() {
		log.Print("Database has backup locks")
	} else {
		log.Print("Database has no backup locks")
	}
	dd := dir_dumper.NewDirDumper(dsn, dir, dbInfo)
	wp := worker_pool.NewPool(streams, dd.Dump)
	names := make(chan interface{})
	go func() {
		for _, tableName := range dbInfo.Tables() {
			if _, ok := skipList[tableName]; !ok {
				names <- tableName
			}
		}
		close(names)
	}()
	wp.Run(names)
}
