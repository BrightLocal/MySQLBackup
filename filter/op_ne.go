package filter

import "github.com/pkg/errors"

// OpNe - !=
type OpNe struct {
	field    string
	argument interface{}
}

func (o OpNe) Type() NodeType {
	return "BoolExpr"
}

func (o OpNe) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '!=' operation")
	} else {
		return value != o.argument, nil
	}
}
