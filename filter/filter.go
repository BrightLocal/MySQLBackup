package filter

import (
	"regexp"

	"github.com/pkg/errors"
)

type FilterSet map[string]*Filter

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
func NewFilterSet(expression string) (FilterSet, error) {
	result := map[string]*Filter{}

	expressionTables := regexp.MustCompile(`\s*,\s*`).Split(expression, -1)
	for _, item := range expressionTables {
		filter, err := NewFilter(item)
		if err != nil {
			return nil, err
		}
		result[filter.tableName] = filter
	}

	return result, nil
}

// NewFilter returns new filter for expression:
// table_name(field == "val")
func NewFilter(expression string) (*Filter, error) {
	f := &Filter{}
	return f, nil
}

func (f *Filter) Passes(data map[string]interface{}) (bool, error) {
	return f.expr.Value(data)
}
