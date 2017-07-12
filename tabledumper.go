package main

import "flag"

func main() {
	var (
		database   string
		username   string
		password   string
		skipTables string
		dir        string
		streams    int
	)
	flag.StringVar(&database, "database", "", "Database name to dump")
	flag.StringVar(&username, "username", "root", "User name")
	flag.StringVar(&password, "password", "", "Password")
	flag.StringVar(&skipTables, "skip-tables", "", "Table names to skip")
	flag.StringVar(&dir, "dir", "", "Destination directory path")
	flag.IntVar(&streams, "streams", 8, "How many tables to dump in parallel")
	flag.Parse()
	// todo
}
