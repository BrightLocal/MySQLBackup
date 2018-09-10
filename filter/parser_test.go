package filter

import (
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	in1 := `table_1(hello == 1 AND (world != 'foo( bar)' OR foo IN(bar, foo))),  table2 (f IS NULL) table3()`
	out := split(in1)
	for key, expression := range out {
		key = strings.TrimSpace(key)
		expression = strings.TrimSpace(expression)
		if rValidKey.MatchString(key) {
			t.Logf("%s > %s", key, expression)
			parse(expression)
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
