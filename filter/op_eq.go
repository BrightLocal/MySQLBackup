package filter

import "github.com/pkg/errors"

// OpEq - =
type OpEq struct {
	field    string
	argument interface{}
}

func (o OpEq) Type() NodeType {
	return "BoolExpr"
}

func (o OpEq) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '=' operation")
	} else {
		return value == o.argument, nil
	}
}
