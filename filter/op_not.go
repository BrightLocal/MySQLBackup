package filter

// OpNot - NOT operation
type OpNot struct {
	x BoolExpr
}

func (o OpNot) Type() NodeType {
	return "BoolExpr"
}

func (o OpNot) Value(data map[string]interface{}) (bool, error) {
	xRes, err := o.x.Value(data)
	return !xRes, err
}
