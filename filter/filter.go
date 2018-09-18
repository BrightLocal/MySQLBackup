package filter

import (
	"github.com/pkg/errors"
)

type FilterSet map[string]BoolExpr

var (
	errFieldNotFound    = errors.New("field not found")
	errTypesMismatch    = errors.New("types mismatch")
	errTypeNotSupported = errors.New("type not supported")
)

// NewFilterSet returns new filters for expression:
// table_name(field == "val"),table02(field02 != "val2" AND field03 == 123)
func NewFilterSet(expression string) (result FilterSet, err error) {
	for table, expr := range split(expression) {
		result[table], err = NewFilter(expr)
		if err != nil {
			return nil, err
		}
	}
	return
}

// NewFilter returns new filter for table expression:
// "table_name", "field == val OR field2 > 5"
func NewFilter(expression string) (BoolExpr, error) {
	return parse(expression)
}
