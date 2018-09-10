package filter

import (
	"github.com/pkg/errors"
)

type FilterSet map[string]Operator

type Filter struct {
	tableName string
	expr      Operator
}

var (
	errFieldNotFound    = errors.New("field not found")
	errTypesMismatch    = errors.New("types mismatch")
	errTypeNotSupported = errors.New("type not supported")
)

// OpAnd Op = "AND"
// OpOr  = "OR"
// OpNot = "NOT"
// OpEq        = "="
// OpNe     = "!="
// OpGt     = ">"
// OpGe     = ">="
// OpLt     = "<"
// OpLe     = "<="
// OpIsNull = "IS NULL"
// OpIn     = "IN"

// NewFilterSet returns new filters for expression:
// table_name(field == "val"),table02(field02 != "val2" AND field03 == 123)
func NewFilterSet(expression string, tableFields map[string][]string) (result FilterSet, err error) {
	for table, expr := range split(expression) {
		if _, ok := tableFields[table]; !ok {
			return nil, errors.New("unknown table " + table)
		}
		result[table], err = NewFilter(expr, tableFields[table])
		if err != nil {
			return nil, err
		}
	}
	return
}

// NewFilter returns new filter for table expression:
// "table_name", "field == val OR field2 > 5"
func NewFilter(expression string, fields []string) (Operator, error) {
	return parse(expression, fields)
}
