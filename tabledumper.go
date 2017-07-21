package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/db_info"
	"github.com/BrightLocal/MySQLBackup/dir_dumper"
	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/worker_pool"
	_ "github.com/go-sql-driver/mysql"
)

type config struct {
	Hostname   string
	Port       int
	Database   string
	Login      string
	Username   string
	Password   string
	Tables     string
	SkipTables string
	Dir        string
	Streams    int
	DSN        string
	RunAfter   string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg := &config{}
	flag.StringVar(&cfg.Hostname, "hostname", "localhost", "Host name")
	flag.IntVar(&cfg.Port, "port", 3306, "Port number")
	flag.StringVar(&cfg.Database, "database", "", "Database name to dump")
	flag.StringVar(&cfg.Login, "login-path", "", "Login path")
	flag.StringVar(&cfg.Username, "username", "", "User name")
	flag.StringVar(&cfg.Password, "password", "", "Password")
	flag.StringVar(&cfg.Tables, "tables", "", "Tables to dump (incompatible with -skip-tables)")
	flag.StringVar(&cfg.SkipTables, "skip-tables", "", "Table names to skip (incompatible with -tables)")
	flag.StringVar(&cfg.Dir, "dir", ".", "Destination directory path")
	flag.StringVar(&cfg.RunAfter, "run-after", "", "Command to run after a file dump (%FILE_NAME% and %FILE_PATH% will be substituted)")
	flag.IntVar(&cfg.Streams, "streams", runtime.NumCPU(), "How many tables to dump in parallel")
	flag.Parse()
	if cfg.Database == "" {
		flag.Usage()
		return
	}
	cfg.buildDSN()
	skipList := make(map[string]struct{})
	if !(cfg.Tables == "" || cfg.SkipTables == "") {
		flag.Usage()
		return
	}
	if cfg.SkipTables != "" {
		for _, t := range strings.Split(cfg.SkipTables, ",") {
			skipList[strings.TrimSpace(t)] = struct{}{}
		}
	}
	dbInfo, err := db_info.New(cfg.DSN)
	if err != nil {
		log.Fatalf("Error connecting to %s: %s", cfg.DSN, err)
	}
	if dbInfo.HasBackupLock() {
		log.Print("Database has backup locks")
	} else {
		log.Print("Database has no backup locks")
	}
	log.Printf("Will use %d streams", cfg.Streams)
	dd := dir_dumper.
		NewDirDumper(cfg.Dir, dbInfo).
		Connect(cfg.DSN).
		RunAfter(cfg.RunAfter)
	wp := worker_pool.NewPool(cfg.Streams, dd.Dump)
	names := make(chan interface{})
	go func() {
		if cfg.Tables == "" {
			// all tables except skipped
			for _, tableName := range dbInfo.Tables() {
				if _, ok := skipList[tableName]; !ok {
					names <- tableName
				}
			}
		} else {
			// specific tables only
			for _, tableName := range strings.Split(cfg.Tables, ",") {
				names <- tableName
			}
		}
		close(names)
	}()
	start := time.Now()
	wp.Run(names)
	dd.PrintStats(cfg.Streams, time.Now().Sub(start))
}

func (c *config) buildDSN() {
	if c.Login != "" {
		var err error
		c.DSN, err = mylogin_reader.Read().GetDSN(c.Login)
		if err != nil {
			log.Fatalf("Error finding MySQL credentials: %s", err)
		}
	} else if c.Username != "" {
		if strings.HasPrefix(c.Hostname, "/") {
			c.DSN = fmt.Sprintf(
				"%s:%s@unix(%s)/",
				c.Username,
				c.Password,
				c.Hostname,
			)
		} else {
			if c.Hostname == "localhost" || c.Hostname == "127.0.0.1" {
				if socket := mylogin_reader.FindSocketFile(); socket != "" {
					c.DSN = fmt.Sprintf(
						"%s:%s@unix(%s)/",
						c.Username,
						c.Password,
						socket,
					)
				}
			} else {
				c.DSN = fmt.Sprintf(
					"%s:%s@tcp(%s:%d)/",
					c.Username,
					c.Password,
					c.Hostname,
					c.Port,
				)
			}
		}
	} else {
		flag.Usage()
		os.Exit(1)
	}
	if c.Database == "" {
		flag.Usage()
		os.Exit(1)
	}
	c.DSN += c.Database + "?charset=utf8"
}
