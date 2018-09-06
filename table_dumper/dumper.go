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
	TableColumnType(string, int) string
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
	w         io.Writer
}

func NewTableDumper(dsn, tableName string, config Config) *Dumper {
	return &Dumper{
		dsn:       dsn,
		tableName: tableName,
		config:    config,
	}
}

func (d *Dumper) Run(w io.Writer, conn *sqlx.DB) (stats, error) {
	d.w = w
	s := stats{}
	log.Printf("Starting dumping table %q", d.tableName)
	query := fmt.Sprintf("SELECT * FROM `%s`", d.tableName)
	result, err := conn.Queryx(query)
	if err != nil {
		return s, err
	}
	defer result.Close()
	start := time.Now()

	columnNames, err := result.Columns()
	if err != nil {
		return s, err
	}
	if err := d.writeHeader(columnNames); err != nil {
		return s, err
	}

	for result.Next() {
		row, err := result.SliceScan()
		if err != nil {
			return s, err
		}
		s.rows++
		b, err := d.compactRow(row)
		if err != nil {
			return s, err
		}
		s.bytes += b
	}
	s.duration = time.Now().Sub(start)
	log.Printf("Finished dumping table %q (%d rows, %d bytes) in %s", d.tableName, s.Rows(), s.Bytes(), s.Duration().String())
	return s, nil
}

func (d *Dumper) writeHeader(columnNames []string) error {
	headerColumns := make([]string, len(columnNames))
	for i, name := range columnNames {
		headerColumns[i] = fmt.Sprintf("`%s`", name)
	}
	if _, err := io.WriteString(d.w, strings.Join(headerColumns, ",")+"\n"); err != nil {
		return err
	}

	return nil
}

func (d *Dumper) compactRow(row []interface{}) (int, error) {
	var n, b int
	var err error
	for col, val := range row {
		if val != nil {
			switch d.config.TableColumnType(d.tableName, col) {
			case "string":
				out, err := json.Marshal(string(val.([]uint8)))
				if err != nil {
					log.Fatalf("Error marshaling value of column %d in table %s[%s]: %s", col, d.tableName, string(row[0].([]byte)), err)
				}
				b, err = d.w.Write(out)
				n += b
			case "binary":
				out, err := json.Marshal(val)
				if err != nil {
					log.Fatalf("Error marshaling value of column %d in table %s[%s]: %s", col, d.tableName, string(row[0].([]byte)), err)
				}
				b, err = d.w.Write(out)
				n += b
			case "numeric":
				b, err = d.w.Write([]byte(val.([]uint8)))
				n += b
			default:
				log.Fatalf("Unsupported column type %q for %s:%d", d.config.TableColumnType(d.tableName, col), d.tableName, col)
			}
		}
		if col != len(row)-1 {
			b, err = d.w.Write([]byte(","))
			n += b
		}
	}
	b, err = d.w.Write([]byte("\n"))
	n += b
	return n, err
}
