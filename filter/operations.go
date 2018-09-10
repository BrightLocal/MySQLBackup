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

// OpGt - >
type OpGt struct {
	field    string
	argument interface{}
}

func (o OpGt) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '>' operation")
	} else {
		switch v := value.(type) {
		case string:
			if arg, ok := o.argument.(string); ok {
				return v > arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case int:
			if arg, ok := o.argument.(int); ok {
				return v > arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case float64:
			if arg, ok := o.argument.(float64); ok {
				return v > arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		default:
			return false, errors.Wrapf(errTypeNotSupported, "%v (%T))", value)
		}
	}
}

// OpLt - <
type OpLt struct {
	field    string
	argument interface{}
}

func (o OpLt) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '<' operation")
	} else {
		switch v := value.(type) {
		case string:
			if arg, ok := o.argument.(string); ok {
				return v < arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case int:
			if arg, ok := o.argument.(int); ok {
				return v < arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case float64:
			if arg, ok := o.argument.(float64); ok {
				return v < arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		default:
			return false, errors.Wrapf(errTypeNotSupported, "%v (%T))", value)
		}
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

	yRes, err := o.y.Value(data)
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

	yRes, err := o.y.Value(data)
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
