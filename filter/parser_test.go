package filter

import (
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	fields := map[string][]string{
		"table_1": {"hello", "world", "foo", "bar"},
	}

	in1 := `table_1(foo == "val" AND world > 233 OR bar != 123)`
	out := split(in1)
	for key, expression := range out {
		key = strings.TrimSpace(key)
		expression = strings.TrimSpace(expression)
		if reValidKey.MatchString(key) {
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
