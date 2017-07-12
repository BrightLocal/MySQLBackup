package table_dumper

import (
	"log"
	"time"
)

type Dumper struct {
	dsn       string
	tableName string
}

func NewTableDumper(dsn, tableName string) *Dumper {
	return &Dumper{
		dsn:       dsn,
		tableName: tableName,
	}
}

func (d *Dumper) Run(directory string) error {
	log.Printf("Starting dumping table %q into directory %q", d.tableName, directory)
	time.Sleep(time.Second)
	log.Printf("Finished dumping table %q into directory %q", d.tableName, directory)
	return nil
}
