package filter

import "github.com/pkg/errors"

// OpIsNull - IS NULL
type OpIsNull struct {
	field string
}

func (o OpIsNull) Type() NodeType {
	return "BoolExpr"
}

func (o OpIsNull) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for 'IS NULL' operation")
	} else {
		return value == nil, nil
	}
}
