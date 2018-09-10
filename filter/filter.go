package filter

import (
	"regexp"
)

type FilterSet map[string]*Filter

type Filter struct {
	tableName string
	expr      Expr
}

type Op string
type OperandType string

const (
	OpAnd Op = "AND"
	OpOr     = "OR"
	OpEq     = "=="
	OpNe     = "!="
)

const (
	OperandField      OperandType = "field"
	OperandValue                  = "value"
	OperandExpression             = "expression"
)

type Expr struct {
	Type  OperandType
	Op    Op
	Name  string      // if Type == "field"
	Value interface{} // if Type == "value"
	X, Y  *Expr       // if Type == "expression"
}

// NewFilterSet returns new filters for expression:
// table_name(field == "val"),table02(field02 != "val2" && field03 == 123)
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
	result := &Filter{}

	tableName, err := result.getTableName()
	if err != nil {
		return nil, err
	}
	result.tableName = tableName

	return result, nil
}

func (f *Filter) getTableName() (string, error) {
	return "table01", nil
}

func (f *Filter) Passes(data map[string]interface{}) bool {
	return true
}
