package main

// Test dump run time: 10:55:57 - 07:58:44

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"

	"github.com/BrightLocal/MySQLBackup/db_info"
	"github.com/BrightLocal/MySQLBackup/dir_dumper"
	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/worker_pool"
	"github.com/go-ini/ini"
	_ "github.com/go-sql-driver/mysql"
)

type config struct {
	Hostname   string
	Port       int
	Database   string
	Login      string
	Username   string
	Password   string
	SkipTables string
	Dir        string
	Streams    int
	DSN        string
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
	flag.StringVar(&cfg.SkipTables, "skip-tables", "", "Table names to skip")
	flag.StringVar(&cfg.Dir, "dir", ".", "Destination directory path")
	flag.IntVar(&cfg.Streams, "streams", runtime.NumCPU(), "How many tables to dump in parallel")
	flag.Parse()
	if cfg.Database == "" {
		flag.Usage()
		return
	}
	cfg.buildDSN()
	skipList := make(map[string]struct{})
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
	dd := dir_dumper.NewDirDumper(cfg.DSN, cfg.Dir, dbInfo)
	wp := worker_pool.NewPool(cfg.Streams, dd.Dump)
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
				if socket := c.findSocketFile(); socket != "" {
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

func (c *config) findSocketFile() string {
	if cfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, os.Getenv("HOME")+"/.my.cnf"); err == nil {
		if socket, err := cfg.Section("client").GetKey("socket"); err == nil {
			if _, err := os.Stat(socket.String()); !os.IsNotExist(err) {
				return socket.String()
			}
			log.Printf("Socket is specified in ~/.my.cnf but not found in the file system")
		}
	}
	if cfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, "/etc/mysql/my.cnf"); err == nil {
		if socket, err := cfg.Section("client").GetKey("socket"); err == nil {
			if _, err := os.Stat(socket.String()); !os.IsNotExist(err) {
				return socket.String()
			}
			log.Printf("Socket is specified in /etc/mysql/my.cnf but not found in the file system")
		}
	}
	return ""
}
