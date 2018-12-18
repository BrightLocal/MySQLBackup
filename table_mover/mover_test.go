package table_mover

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/kr/pretty"
)

func TestTable(t *testing.T) {
	m := New("", "")
	db, err := sqlx.Connect("mysql", "bl:TmkhcS4Ft7MkC2Hq@tcp(host01)/brightlocal")
	if err != nil {
		t.Error(err)
	}
	table1, err := m.getTable("lsrc_rankings_shard_1", db)
	if err != nil {
		t.Error(err)
	}
	table2, err := m.getTable("lsrc_rankings_shard_2", db)
	if err != nil {
		t.Error(err)
	}
	if pk := table1.PK(); pk != "ranking_id" {
		t.Errorf("Expected 'ranking_id', got %q", pk)
	}
	if ok, err := table1.Identical(table2); !ok {
		t.Errorf("Not identical: %s", err)
		t.Logf("%# v", pretty.Formatter(table1))
		t.Logf("%# v", pretty.Formatter(table2))
	}
	t.Logf("%s", table1.insert())
}
