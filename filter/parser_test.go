package filter

import (
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	fields := map[string][]string{
		"table_1": {"hello", "world", "foo", "bar"},
	}
	//in1 := `table_1(hello == 1 AND (world != 'foo( bar)' OR foo IN(bar, foo)))`
	in1 := `table_1(foo IN ('bar', 'f\'oo',123) OR foo IS NULL  AND foo > 5) AND bar == foo)`
	out := split(in1)
	for key, expression := range out {
		key = strings.TrimSpace(key)
		expression = strings.TrimSpace(expression)
		if rValidKey.MatchString(key) {
			if _, err := parse(expression, fields[key]); err != nil {
				t.Error(err)
			}
		} else {
			t.Errorf("Invalid key %q", key)
		}
	}
}

func TestSubstr(t *testing.T) {
	str := "hello world"
	if h := substr(&str, 0, 5); h != "hello" {
		t.Errorf("Expected %q, got %q", "hello", h)
	}
	if h := substr(&str, 6, 10); h != "world" {
		t.Errorf("Expected %q, got %q", "world", h)
	}
}
