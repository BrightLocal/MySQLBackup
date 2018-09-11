package filter

// OpNot - NOT operation
type OpNot struct {
	x Operator
}

func (o OpNot) Value(data map[string]interface{}) (bool, error) {
	xRes, err := o.x.Value(data)
	return !xRes, err
}
