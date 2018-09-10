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
	if f.expr.Type != OperandExpression || f.expr.X == nil || f.expr.Y == nil {
		// TODO: return error?
		return false
	}

	return f.expr.eval(data)
}

func (expr Expr) eval(data map[string]interface{}) bool {
	if expr.Type == OperandExpression {
		if expr.X == nil || expr.Y == nil {
			// TODO: error in expression
			return false
		}

		switch expr.Op {
		case OpEq, OpNe:

			var x, y interface{}

			switch expr.X.Type {
			case OperandField:
				// TODO: check exists field in map
				x = data[expr.X.Name]
			case OperandValue:
				x = expr.X.Value
			default:
				// TODO error
				return false
			}
			switch expr.Y.Type {
			case OperandField:
				// TODO: check exists field in map
				y = data[expr.Y.Name]
			case OperandValue:
				y = expr.Y.Value
			default:
				// TODO error
				return false
			}

			if expr.Op == OpEq {
				return x == y
			}

			return x != y

		case OpAnd, OpOr:
			if expr.X.Type == OperandExpression && expr.Y.Type == OperandExpression {
				if expr.Op == OpAnd {
					return expr.X.eval(data) && expr.Y.eval(data)
				}
				return expr.X.eval(data) || expr.Y.eval(data)

			} else {
				// TODO error
				return false
			}
		}
	} else {
		return false
	}

	return false
}
