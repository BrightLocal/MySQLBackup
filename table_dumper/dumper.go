package table_dumper

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type Config interface {
	HasBackupLock() bool
}

type stats struct {
	rows     int
	bytes    int
	duration time.Duration
}

func (s stats) Rows() int               { return s.rows }
func (s stats) Bytes() int              { return s.bytes }
func (s stats) Duration() time.Duration { return s.duration }

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

func (d *Dumper) Run(w io.Writer) (stats, error) {
	s := stats{}
	log.Printf("Starting dumping table %q", d.tableName)
	conn, err := sqlx.Connect("mysql", d.dsn)
	if err != nil {
		return s, err
	}
	defer conn.Close()
	if d.config != nil && d.config.HasBackupLock() {
		if _, err := conn.Exec("LOCK TABLES FOR BACKUP"); err != nil {
			return s, err
		}
		if _, err := conn.Exec("LOCK BINLOG FOR BACKUP"); err != nil {
			return s, err
		}
	}
	if _, err := conn.Exec("START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		return s, err
	}
	if d.config != nil && d.config.HasBackupLock() {
		if _, err := conn.Exec("UNLOCK TABLES"); err != nil {
			return s, err
		}
		if _, err := conn.Exec("UNLOCK BINLOG"); err != nil {
			return s, err
		}
	}
	query := fmt.Sprintf("SELECT * FROM `%s`", d.tableName)
	result, err := conn.Queryx(query)
	if err != nil {
		return s, err
	}
	defer result.Close()
	start := time.Now()
	for result.Next() {
		row, err := result.SliceScan()
		if err != nil {
			return s, err
		}
		s.rows++
		b, err := w.Write([]byte(d.formatRow(row)))
		if err != nil {
			return s, err
		}
		s.bytes += b
	}
	s.duration = time.Now().Sub(start)
	log.Printf("Finished dumping table %q (%d rows, %d bytes) in %s", d.tableName, s.Rows(), s.Bytes(), s.Duration().String())
	return s, nil
}

func (d *Dumper) formatRow(row []interface{}) string {
	var result []string
	for col, r := range row {
		switch val := r.(type) {
		case []uint8:
			out, err := json.Marshal(string(val))
			if err != nil {
				log.Fatalf("Error marshaling value of column %d in table %s[%s]: %s", col, d.tableName, string(row[0].([]byte)), err)
			}
			result = append(result, string(out))
		case nil:
			result = append(result, "null")
		default:
			log.Fatalf("Got unexpected type of column %d in table %s[%s]: %# v", col, d.tableName, string(row[0].([]byte)), r)
		}
	}
	return strings.Join(result, ",") + "\n"
}
