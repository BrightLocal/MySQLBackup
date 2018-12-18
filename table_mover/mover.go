package table_mover

import (
	"fmt"
	"github.com/kr/pretty"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Mover struct {
	srcDSN     string
	dstDSN     string
	src        *sqlx.DB
	dst        *sqlx.DB
	log        *log.Logger
	stmtExists *sqlx.Stmt
	stmtSelect *sqlx.Stmt
	stmtInsert *sqlx.Stmt
}

func New(src, dst string) *Mover {
	return &Mover{
		srcDSN: src,
		dstDSN: dst,
	}
}

const pageSize = 1000

/*
[*] Check if src table exists
[*] Check if dst table exists
[ ] Create dst table if does not exist
[*] Compare src and dst schema
[*] Detect PK column
[*] Select all PKs from src table
[*] Check is PK present in dst table
[*] Copy missing rows from src to dst
*/
func (m *Mover) Move(table string) error {
	m.log = log.New(os.Stdout, fmt.Sprintf("[%s] ", table), log.Ltime|log.Lmicroseconds|log.Lshortfile)
	m.connect()
	var err error
	srcTable, err := m.getTable(table, m.src)
	if err != nil {
		return errors.Wrapf(err, "error reading source table %q", table)
	}
	m.log.Printf("Detected primary key %q", srcTable.Primary)
	dstTable, err := m.getTable(table, m.dst)
	if err != nil {
		return errors.Wrapf(err, "error reading destination table %q", table)
	}
	if ok, err := srcTable.Identical(dstTable); !ok {
		return errors.Wrapf(err, "tables %s are not identical", table)
	}
	// Ready to copy
	if m.stmtExists, err = m.dst.Preparex(
		fmt.Sprintf( //language=MySQL
			"SELECT COUNT(*) FROM `%s` WHERE `%s` = ?",
			dstTable.Name, srcTable.PK(),
		),
	); err != nil {
		return err
	}
	if m.stmtSelect, err = m.src.Preparex(
		fmt.Sprintf( //language=MySQL
			"SELECT * FROM `%s` WHERE `%s` = ? LIMIT 1",
			srcTable.Name, srcTable.PK(),
		),
	); err != nil {
		return err
	}
	if m.stmtInsert, err = m.dst.Preparex(dstTable.insert()); err != nil {
		return err
	}
	var startIndex *string
	if err := m.dst.QueryRow(
		fmt.Sprintf( //language=MySQL
			"SELECT MAX(`%s`) FROM `%s`",
			srcTable.PK(), dstTable.Name),
	).Scan(&startIndex); err != nil {
		return err
	}
	if startIndex == nil {
		zero := "0"
		startIndex = &zero
	}
	m.log.Printf("Starting with pk %s", *startIndex)
	pks := make(chan string)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if err := m.readPKs(srcTable, *startIndex, pks); err != nil {
			m.log.Printf("Error reading PKs: %s", err)
		}
		close(pks)
		wg.Done()
	}()
	n := 0
	t := 0
	defer m.log.Printf("Moved %d rows", n)
	tick := time.Now().Add(time.Minute)
	for pk := range pks {
		if !m.pkExists(pk) {
			if err := m.migratePK(pk); err != nil {
				return errors.Wrapf(err, "error migrating pk %s", pk)
			}
			n++
		}
		t++
		if time.Now().After(tick) {
			m.log.Printf("Moved %d rows, skipped %d, lastest key: %s", n, t-n, pk)
			tick = time.Now().Add(time.Minute)
		}
	}
	wg.Wait()
	return nil
}

func (m *Mover) connect() {
	var err error
	m.src, err = sqlx.Connect("mysql", m.srcDSN)
	if err != nil {
		m.log.Fatalf("Error connecting to source database: %s", err)
	}
	if err = m.src.Ping(); err != nil {
		m.log.Fatalf("Error pinging source database: %s", err)
	}
	m.dst, err = sqlx.Connect("mysql", m.dstDSN)
	if err != nil {
		m.log.Fatalf("Error connecting to destination database: %s", err)
	}
	if err = m.dst.Ping(); err != nil {
		m.log.Fatalf("Error pinging destination database: %s", err)
	}
}

func (Mover) getTable(tableName string, db *sqlx.DB) (*table, error) {
	rows, err := db.Query(fmt.Sprintf("EXPLAIN `%s`", tableName))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var t table
	t.Name = tableName
	for rows.Next() {
		var (
			field string
			kind  string
			null  string
			key   string
			def   *string
			extra string
		)
		if err := rows.Scan(&field, &kind, &null, &key, &def, &extra); err != nil {
			return nil, err
		}
		t.Columns = append(t.Columns, column{
			Field:   field,
			Type:    kind,
			Null:    null == "YES",
			Key:     key,
			Default: def,
			Extra:   extra,
		})
		if key == "PRI" {
			t.Primary = field
		}
	}
	return &t, nil
}

func (m *Mover) readPKs(t *table, start interface{}, emit chan string) error {
	var max int
	if err := m.src.QueryRow(
		fmt.Sprintf( //language=MySQL
			"SELECT MAX(`%s`) FROM `%s`",
			t.PK(), t.Name,
		),
	).Scan(&max); err != nil {
		return err
	}
	pagedQuery := fmt.Sprintf( //language=MySQL
		"SELECT `%s` FROM `%s` WHERE `%s` > ? ORDER BY `%s` LIMIT ? OFFSET ?",
		t.PK(), t.Name, t.PK(), t.PK(),
	)
	for offset := 0; offset < max; offset += pageSize {
		rows, err := m.src.Query(pagedQuery, start, pageSize, offset)
		if err != nil {
			return err
		}
		for rows.Next() {
			var col string
			if err := rows.Scan(&col); err != nil {
				return err
			}
			emit <- col
		}
		_ = rows.Close()
	}
	return nil
}

func (m *Mover) pkExists(pk string) bool {
	var count int
	if err := m.stmtExists.QueryRowx(pk).Scan(&count); err != nil {
		m.log.Fatalf("Error scanning: %s", err)
	}
	return count > 0
}

func (m *Mover) migratePK(pk string) (err error) {
	cols, err := m.stmtSelect.QueryRowx(pk).SliceScan()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && strings.Contains(err.Error(), "truncated") {
			m.log.Printf("Failed row: %# v", pretty.Formatter(cols))
		}
	} ()
	_, err = m.stmtInsert.Exec(cols...)
	return err
}
