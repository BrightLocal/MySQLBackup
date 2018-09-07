package filter

type FilterSet map[string]*Filter

type Filter struct {
}

func New(expression string) (FilterSet, error) {
	result := map[string]*Filter{}
	return result, nil
}

func (f *Filter) Passes(data map[string]interface{}) bool {
	return true
}
