package filter

import "github.com/pkg/errors"

type Operator interface {
	Value(data map[string]interface{}) (bool, error)
}

// OpEq - =
type OpEq struct {
	field    string
	argument interface{}
}

func (o OpEq) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '=' operation")
	} else {
		return value == o.argument, nil
	}
}

// OpNe - !=
type OpNe struct {
	field    string
	argument interface{}
}

func (o OpNe) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '!=' operation")
	} else {
		return value != o.argument, nil
	}
}

// OpAnd - AND operation
type OpAnd struct {
	x, y Operator
}

func (o OpAnd) Value(data map[string]interface{}) (bool, error) {
	xRes, err := o.x.Value(data)
	if err != nil {
		return false, err
	}

	yRes, err := o.x.Value(data)
	if err != nil {
		return false, err
	}

	return xRes && yRes, nil
}

// OpOr - OR operation
type OpOr struct {
	x, y Operator
}

func (o OpOr) Value(data map[string]interface{}) (bool, error) {
	xRes, err := o.x.Value(data)
	if err != nil {
		return false, err
	}

	yRes, err := o.x.Value(data)
	if err != nil {
		return false, err
	}

	return xRes || yRes, nil
}

// OpNot - NOT operation
type OpNot struct {
	x Operator
}

func (o OpNot) Value(data map[string]interface{}) (bool, error) {
	xRes, err := o.x.Value(data)
	if err != nil {
		return false, err
	}

	return !xRes, nil
}
