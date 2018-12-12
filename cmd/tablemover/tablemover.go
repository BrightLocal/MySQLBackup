package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/BrightLocal/MySQLBackup/mylogin_reader"
	"github.com/BrightLocal/MySQLBackup/table_mover"
)

type hostConfig struct {
	Hostname string
	Port     int
	Database string
	Login    string
	Username string
	Password string
	DSN      string
}

type moverConfig struct {
	Src    hostConfig
	Dst    hostConfig
	Tables []string
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg := &moverConfig{}
	flag.StringVar(&cfg.Src.Hostname, "src-host", "localhost", "Source host name")
	flag.StringVar(&cfg.Dst.Hostname, "dst-host", "localhost", "Destination host name")
	flag.IntVar(&cfg.Src.Port, "src-port", 3306, "Source port number")
	flag.IntVar(&cfg.Dst.Port, "dst-port", 3306, "Destination port number")
	flag.StringVar(&cfg.Src.Database, "src-db", "", "Source database name")
	flag.StringVar(&cfg.Dst.Database, "dst-db", "", "Destination database name")
	flag.StringVar(&cfg.Src.Login, "src-login", "", "Source db login path")
	flag.StringVar(&cfg.Dst.Login, "dst-login", "", "Destination db login path")
	flag.StringVar(&cfg.Src.Username, "src-user", "root", "Source db user name (incompatible with -src-login)")
	flag.StringVar(&cfg.Dst.Username, "dst-user", "root", "Destination db user name (incompatible with -dst-login)")
	flag.StringVar(&cfg.Src.Password, "src-password", "", "Source db password")
	flag.StringVar(&cfg.Dst.Password, "dst-password", "", "Destination db password")
	var tables string
	flag.StringVar(&tables, "tables", "", "Comma separated list of tables to copy")
	flag.Parse()
	if tables == "" {
		print("No tables specified\n")
		flag.Usage()
		os.Exit(1)
	}
	cfg.Tables = strings.Split(tables, ",")
	if cfg.Src.Login == "" && cfg.Src.Username == "" {
		print("Use either login or username/password for source database\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.Dst.Login == "" && cfg.Dst.Username == "" {
		print("Use either login or username/password for destination database\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.Src.Login != "" {
		reader := mylogin_reader.Read()
		var err error
		cfg.Src.DSN, err = reader.GetDSN(cfg.Src.Login)
		if err != nil {
			fmt.Printf("Error reading login: %s", err)
			os.Exit(1)
		}
	} else {
		if cfg.Src.Password == "" {
			cfg.Src.DSN = fmt.Sprintf("%s@tcp(%s:%d)/%s", cfg.Src.Username, cfg.Src.Hostname, cfg.Src.Port, cfg.Src.Database)
		} else {
			cfg.Src.DSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.Src.Username, cfg.Src.Password, cfg.Src.Hostname, cfg.Src.Port, cfg.Src.Database)
		}
	}
	if cfg.Dst.Login != "" {
		reader := mylogin_reader.Read()
		var err error
		cfg.Dst.DSN, err = reader.GetDSN(cfg.Dst.Login)
		if err != nil {
			fmt.Printf("Error reading login: %s", err)
			os.Exit(1)
		}
	} else {
		if cfg.Dst.Password == "" {
			cfg.Dst.DSN = fmt.Sprintf("%s@tcp(%s:%d)/%s", cfg.Dst.Username, cfg.Dst.Hostname, cfg.Dst.Port, cfg.Dst.Database)
		} else {
			cfg.Dst.DSN = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.Dst.Username, cfg.Dst.Password, cfg.Dst.Hostname, cfg.Dst.Port, cfg.Dst.Database)
		}
	}
	wg := sync.WaitGroup{}
	for _, table := range cfg.Tables {
		wg.Add(1)
		go func(t string) {
			start := time.Now()
			if err := table_mover.New(cfg.Src.DSN, cfg.Dst.DSN).Move(t); err != nil {
				log.Printf("Error moving table %q: %s", t, err)
			} else {
				log.Printf("Table %q moved successfully in %s", t, time.Now().Sub(start))
			}
			wg.Done()
		}(table)
	}
	wg.Wait()
}
