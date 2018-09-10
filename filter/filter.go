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
	OpAnd    Op = "AND"
	OpOr        = "OR"
	OpEq        = "="
	OpNe        = "!="
	OpGt        = ">"
	OpGe        = ">="
	OpLt        = "<"
	OpLe        = "<="
	OpNot       = "NOT"
	OpIsNull    = "IS NULL"
	OpIn        = "IN"
)

const (
	OperandField      OperandType = "field"
	OperandValue                  = "value"
	OperandExpression             = "expression"
)

type Expr struct {
	Type     OperandType
	Op       Op
	Name     string      // if Type == "field"
	Value    interface{} // if Type == "value"
	Operands []Expr      // if Type == "expression"
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
	f := &Filter{}
	return f, nil
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
	if len(expr.Operands) != 2 { // TODO refactor hardcode
		return false, errOperandNotFound
	}

	switch expr.Op {
	case OpEq, OpNe:
		var (
			x, y interface{}
			ok   bool
		)

		switch expr.Operands[0].Type {
		case OperandField:
			x, ok = data[expr.Operands[0].Name]
			if !ok {
				return false, errors.Wrap(errFieldNotFound, "for first argument")
			}
		case OperandValue:
			x = expr.Operands[0].Value
		default:
			return false, errors.Wrapf(errInvalidOperandType, "for first operand")
		}
		switch expr.Operands[1].Type {
		case OperandField:
			y, ok = data[expr.Operands[1].Name]
			if !ok {
				return false, errors.Wrap(errFieldNotFound, "for second argument")
			}
		case OperandValue:
			y = expr.Operands[1].Value
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
		xRes, err := expr.Operands[0].eval(data)
		if err != nil {
			return false, err
		}
		yRes, err := expr.Operands[1].eval(data)
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
