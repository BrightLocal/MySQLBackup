package table_dumper

import (
	"fmt"
	"log"

	"encoding/json"
	"strings"

	"time"

	"github.com/jmoiron/sqlx"
	"io"
)

type Config interface {
	HasBackupLock() bool
}

type Dumper struct {
	dsn       string
	tableName string
	config    Config
}

func NewTableDumper(dsn, tableName string, config Config) *Dumper {
	return &Dumper{
		dsn:       dsn,
		tableName: tableName,
		config:    config,
	}
}

func (d *Dumper) Run(w io.Writer) error {
	log.Printf("Starting dumping table %q", d.tableName)
	//if d.config != nil && d.config.HasBackupLock() {
	//	log.Printf("LOCK TABLES FOR BACKUP")
	//	log.Printf("LOCK BINLOG FOR BACKUP")
	//}
	//log.Printf("START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */")
	//if d.config != nil && d.config.HasBackupLock() {
	//	log.Printf("UNLOCK TABLES /* trx-only */")
	//	log.Printf("UNLOCK BINLOG")
	//}

	query := fmt.Sprintf("SELECT * FROM `%s`", d.tableName)
	log.Printf("%s", query)
	conn, err := sqlx.Connect("mysql", d.dsn)
	if err != nil {
		return err
	}
	result, err := conn.Queryx(query)
	if err != nil {
		return err
	}
	defer result.Close()
	n := 0
	start := time.Now()
	for result.Next() {
		row, err := result.SliceScan()
		if err != nil {
			return err
		}
		n++
		log.Printf("%s", d.formatRow(row))
	}
	log.Printf("Finished dumping table %q (%d rows) in %s", d.tableName, n, time.Now().Sub(start).String())
	return nil
}

func (d *Dumper) formatRow(row []interface{}) string {
	var result []string
	for _, r := range row {
		switch val := r.(type) {
		case []uint8:
			out, err := json.Marshal(string(val))
			if err != nil {
				log.Fatalf("Error marshalling value: %s", err)
			}
			result = append(result, string(out))
		case nil:
			result = append(result, "null")
		default:
			log.Fatalf("%# v", r)
		}
	}
	return strings.Join(result, ",")
}
