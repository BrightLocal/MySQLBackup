package filter

// OpNop
type OpNop struct{}

func (o OpNop) Type() NodeType {
	return "BoolExpr"
}

func (o OpNop) Value(data map[string]interface{}) (bool, error) {
	return true, nil
}
