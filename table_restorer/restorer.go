package table_restorer

import (
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/filter"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
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
	dryRun    bool
	filter    *filter.Filter
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

func (r *Restorer) WithDryRun(dryRun bool) *Restorer {
	r.dryRun = dryRun
	return r
}

func (r *Restorer) WithFilter(filter *filter.Filter) *Restorer {
	r.filter = filter
	return r
}

func (r *Restorer) Run(in io.Reader, conn *sqlx.DB) (stats, error) {
	log.Printf("Restoring table %s: %s", r.tableName, strings.Join(r.columns, ", "))
	statement, err := conn.Prepare(r.query)
	if err != nil {
		return stats{}, err
	}
	l := NewReader(in)
	rows := make(chan []interface{})
	go l.Parse(rows)
	for row := range rows {
		dataAsMap, err := r.getDataAsMap(row)
		if err != nil {
			log.Printf("Warning: %s", err)
			continue
		}
		if r.filter != nil && !r.filter.Passes(dataAsMap) {
			continue // skip row by filter expression
		}

		if r.dryRun {
			fmt.Printf("INSERT: %+v\n", row)
		} else {
			if _, err := statement.Exec(row...); err != nil {
				log.Printf("Warning: error executing query for table %s: %s\n%# v", r.tableName, err, row)
			}
		}
	}
	return stats{}, nil
}

func (r *Restorer) getDataAsMap(data []interface{}) (map[string]interface{}, error) {
	if len(data) != r.colNum {
		return nil, errors.Errorf("column number in table %q mismatch, expected %d, got %d", r.tableName, r.colNum, len(data))
	}

	result := map[string]interface{}{}
	for i, item := range data {
		result[r.columns[i]] = item
	}

	return result, nil
}
