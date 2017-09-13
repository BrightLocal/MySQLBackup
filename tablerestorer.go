package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/dir_restorer"
	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/worker_pool"
)

type restorerConfig struct {
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
	Create     bool
	Truncate   bool
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg := &restorerConfig{}
	flag.StringVar(&cfg.Hostname, "hostname", "localhost", "Host name")
	flag.IntVar(&cfg.Port, "port", 3306, "Port number")
	flag.StringVar(&cfg.Database, "database", "", "Database name to restore")
	flag.StringVar(&cfg.Login, "login-path", "", "Login path")
	flag.StringVar(&cfg.Username, "username", "", "User name")
	flag.StringVar(&cfg.Password, "password", "", "Password")
	flag.StringVar(&cfg.Tables, "tables", "", "Tables to restore (incompatible with -skip-tables)")
	flag.StringVar(&cfg.SkipTables, "skip-tables", "", "Table names to skip (incompatible with -tables)")
	flag.StringVar(&cfg.Dir, "dir", ".", "Source directory path")
	flag.BoolVar(&cfg.Create, "create", false, "Create tables if they do not exist")
	flag.BoolVar(&cfg.Truncate, "truncate", false, "Clear tables before restoring")
	flag.IntVar(&cfg.Streams, "streams", runtime.NumCPU(), "How many tables to restore in parallel")
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
	dr := dir_restorer.
		NewDirRestorer(cfg.Dir).
		Connect(cfg.DSN, cfg.Database).
		CreateTables(cfg.Create).
		TruncateTables(cfg.Truncate)
	if err := dr.Prepare(); err != nil {
		log.Fatalf("error preparing database: %s", err)
	}
	wp := worker_pool.NewPool(cfg.Streams, dr.Restore)
	names := make(chan interface{})
	go func() {
		if cfg.Tables == "" {
			// all tables except skipped
			for _, tableName := range dr.Tables() {
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
	if err := dr.Finish(); err != nil {
		log.Fatalf("error doing final tasks: %s", err)
	}
	dr.PrintStats(cfg.Streams, time.Now().Sub(start))
}

func (c *restorerConfig) buildDSN() {
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
