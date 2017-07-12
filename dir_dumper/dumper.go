package dir_dumper

import (
	"log"

	"github.com/BrightLocal/MySQLBackup/table_dumper"
)

type DirDumper struct {
	dsn    string
	dir    string
	config table_dumper.Config
}

func NewDirDumper(dsn, dir string, config table_dumper.Config) *DirDumper {
	return &DirDumper{
		dsn:    dsn,
		dir:    dir,
		config: config,
	}
}

func (d *DirDumper) Dump(tableName interface{}) {
	name := tableName.(string)
	td := table_dumper.NewTableDumper(d.dsn, name, d.config)
	if err := td.Run(d.dir); err != nil {
		log.Printf("Error running worker: %s", err)
	}
}
