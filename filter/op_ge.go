package filter

import "github.com/pkg/errors"

// OpGe - >=
type OpGe struct {
	field    string
	argument interface{}
}

func (o OpGe) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '>=' operation")
	} else {
		switch v := value.(type) {
		case string:
			if arg, ok := o.argument.(string); ok {
				return v >= arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case int:
			if arg, ok := o.argument.(int); ok {
				return v >= arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		case float64:
			if arg, ok := o.argument.(float64); ok {
				return v >= arg, nil
			}
			return false, errors.Wrapf(errTypesMismatch, "%[1]v (%[1]T) != %[2]v (%[2]T)", value, o.argument)
		default:
			return false, errors.Wrapf(errTypeNotSupported, "%[1]v (%[1]T)", value)
		}
	}
}
