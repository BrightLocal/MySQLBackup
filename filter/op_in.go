package filter

import "github.com/pkg/errors"

// OpIn - IN
type OpIn struct {
	field     string
	arguments []interface{}
}

func (o OpIn) Type() NodeType {
	return "BoolExpr"
}

func (o OpIn) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for 'IN' operation")
	} else {
		for _, item := range o.arguments {
			if value == item {
				return true, nil
			}
		}
		return false, nil
	}
}
