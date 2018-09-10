package filter

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
