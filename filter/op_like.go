package filter

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// OpLike - "field LIKE '%ww_ww%'"
type OpLike struct {
	field string
	re    *regexp.Regexp
}

func NewOpLike(field, reSource string) (Node, error) {
	reStr := strings.Replace(strings.Replace(regexp.QuoteMeta(reSource), "_", ".", -1), "%", ".*", -1)
	re, err := regexp.Compile(reStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to compile regexp (src: %s, re: %s)", reSource, reStr)
	}

	return OpLike{
		field: field,
		re:    re,
	}, nil
}

func (o OpLike) Type() NodeType {
	return "BoolExpr"
}

func (o OpLike) Value(data map[string]interface{}) (bool, error) {
	if value, ok := data[o.field]; !ok {
		return false, errors.Wrapf(errFieldNotFound, "for '=' operation")
	} else {
		if strValue, ok := value.(string); ok {
			return o.re.MatchString(strValue), nil
		}
		return false, errors.Errorf("regexp argument: %[1]v (%[1]T) must have a string type", value)
	}
}
