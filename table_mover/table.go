package table_mover

import (
	"errors"
	"fmt"
	"strings"
)

type column struct {
	Field   string
	Type    string
	Null    bool
	Key     string
	Default *string
	Extra   string
}

// Same enough, not completely
func (c column) Same(col column) bool {
	return c.Field == col.Field &&
		c.Type == col.Type &&
		c.Null == col.Null
}

type table struct {
	Name    string
	Columns []column
	Primary string
}

func (t table) PK() string {
	for _, col := range t.Columns {
		if col.Key == "PRI" {
			return col.Field
		}
	}
	return ""
}

func (t table) Identical(other *table) (bool, error) {
	if len(t.Columns) != len(other.Columns) {
		return false, errors.New("different number or Columns")
	}
	for i, col := range t.Columns {
		if same := col.Same(other.Columns[i]); !same {
			return false, fmt.Errorf("column %s is different", col.Field)
		}
	}
	return true, nil
}

func (t table) insert() string {
	q := "INSERT INTO `" + t.Name + "` ("
	cols := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		cols[i] = "`" + col.Field + "`"
	}
	q += strings.Join(cols, ",")
	return q + ")VALUES(" + strings.Trim(strings.Repeat("?,", len(cols)), ",") + ")"
}
