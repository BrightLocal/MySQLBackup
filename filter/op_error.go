package filter

import "github.com/pkg/errors"

type OpError struct {
	errorMsg string
}

func (o OpError) Type() NodeType {
	return "BoolExpr"
}

func (o OpError) Value(data map[string]interface{}) (bool, error) {
	return false, errors.New(o.errorMsg)
}
