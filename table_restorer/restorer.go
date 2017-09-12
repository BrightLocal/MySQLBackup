package table_restorer

import (
	"io"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type stats struct {
	rows     int
	bytes    int
	duration time.Duration
}

func (s stats) Rows() int               { return s.rows }
func (s stats) Bytes() int              { return s.bytes }
func (s stats) Duration() time.Duration { return s.duration }

type Restorer struct {
	dsn       string
	tableName string
	columns   []string
	colNum    int
	query     string
}

func New(dsn, tableName string, columns []string) *Restorer {
	r := &Restorer{
		dsn:       dsn,
		tableName: tableName,
		columns:   columns,
		colNum:    len(columns),
	}
	r.query = "INSERT INTO `" + tableName + "` ("
	cols := make([]string, len(columns), len(columns))
	vals := make([]string, len(columns), len(columns))
	for i, col := range columns {
		cols[i] = "`" + col + "`"
		vals[i] = "?"
	}
	r.query += strings.Join(cols, ",") + ") VALUES (" + strings.Join(vals, ",") + ")"
	return r
}

func (r *Restorer) Run(in io.Reader, conn *sqlx.DB) (stats, error) {
	log.Printf("Restoring table %s: %s", r.tableName, strings.Join(r.columns, ", "))
	log.Printf("%s", r.query)
	l := NewReader(in)
	rows := make(chan []string)
	go l.Parse(rows)
	for row := range rows {
		if len(row) != r.colNum {
			log.Fatalf("Column number in table %q mismatch, expected %d, got %d (%s)", r.tableName, r.colNum, len(row), row[0])
		}
		// TODO
	}
	return stats{}, nil
}
