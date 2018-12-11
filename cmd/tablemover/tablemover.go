package main

import (
	"flag"
	"log"
)

type hostConfig struct {
	Hostname string
	Port     int
	Database string
	Login    string
	Username string
	Password string
	Tables   []string
}

type moverConfig struct {
	Src hostConfig
	Dst hostConfig
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
	flag.Parse()
	// TODO
}
