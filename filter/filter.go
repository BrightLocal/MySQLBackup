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
	errExpectedExpression   = errors.New("expected expression")
	errOperandNotFound      = errors.New("operand not found")
	errFieldNotFound        = errors.New("field not found")
	errInvalidOperandType   = errors.New("invalid operand type")
	errInvalidOperationType = errors.New("invalid operation type")
)

func (expr Expr) eval(data map[string]interface{}) (bool, error) {
	if expr.Type != OperandExpression {
		return false, errors.Wrapf(errExpectedExpression, "but found: %v", expr.Type)
	}
	if expr.X == nil || expr.Y == nil {
		return false, errOperandNotFound
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

		switch expr.Op {
		case OpEq:
			return x == y, nil
		case OpNe:
			return x != y, nil
		default:
			return false, errors.Wrapf(errInvalidOperationType, "but found: %q", expr.Op)
		}

	case OpAnd, OpOr:
		xRes, err := expr.X.eval(data)
		if err != nil {
			return false, err
		}
		yRes, err := expr.Y.eval(data)
		if err != nil {
			return false, err
		}

		switch expr.Op {
		case OpAnd:
			return xRes && yRes, nil
		case OpOr:
			return xRes || yRes, nil
		default:
			return false, errors.Wrapf(errInvalidOperationType, "but found: %q", expr.Op)
		}

	default:
		return false, errors.Wrapf(errInvalidOperationType, "but found: %q", expr.Op)
	}
}
