package filter

import (
	"regexp"

	"github.com/pkg/errors"
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

func (f *Filter) Passes(data map[string]interface{}) (bool, error) {
	return f.expr.eval(data)
}

var (
	errExpectedExpression = errors.New("expected expression")
	errOperatorNotFound   = errors.New("operator not found")
	errFieldNotFound      = errors.New("field not found")
	errInvalidOperandType = errors.New("invalid operand type")
)

func (expr Expr) eval(data map[string]interface{}) (bool, error) {
	if expr.Type != OperandExpression {
		return false, errors.Wrapf(errExpectedExpression, "but found: %v", expr.Type)
	}
	if expr.X == nil || expr.Y == nil {
		return false, errOperatorNotFound
	}

	switch expr.Op {
	case OpEq, OpNe:

		var (
			x, y interface{}
			ok   bool
		)

		switch expr.X.Type {
		case OperandField:
			x, ok = data[expr.X.Name]
			if !ok {
				return false, errors.Wrap(errFieldNotFound, "for fisrt argument")
			}
		case OperandValue:
			x = expr.X.Value
		default:
			return false, errors.Wrapf(errInvalidOperandType, "for first operand")
		}
		switch expr.Y.Type {
		case OperandField:
			y, ok = data[expr.Y.Name]
			if !ok {
				return false, errors.Wrap(errFieldNotFound, "for second argument")
			}
		case OperandValue:
			y = expr.Y.Value
		default:
			return false, errors.Wrapf(errInvalidOperandType, "for second operand")
		}

		if expr.Op == OpEq {
			return x == y, nil
		}
		return x != y, nil

	case OpAnd, OpOr:
		if expr.X.Type == OperandExpression && expr.Y.Type == OperandExpression {
			xRes, err := expr.X.eval(data)
			if err != nil {
				return false, err
			}
			yRes, err := expr.Y.eval(data)
			if err != nil {
				return false, err
			}

			if expr.Op == OpAnd {
				return xRes && yRes, nil
			}
			return xRes || yRes, nil

		} else {
			return false, errors.Wrapf(errExpectedExpression, "for AND/OR operation, but found: X: %v, Y: %v", expr.X.Type, expr.Y.Type)
		}
	default:
		return false, errInvalidOperandType
	}
}
