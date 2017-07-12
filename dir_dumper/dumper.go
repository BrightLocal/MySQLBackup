package dir_dumper

import "github.com/BrightLocal/MySQLBackup/table_dumper"

type DirDumper struct {
	dsn string
	dir string
}

func NewDirDumper(dsn, dir string) *DirDumper {
	return &DirDumper{
		dsn: dsn,
		dir: dir,
	}
}

func (d *DirDumper) Dump(tableName interface{}) {
	name := tableName.(string)
	td := table_dumper.NewTableDumper(d.dsn, name)
	td.Run(d.dir)
}
