package filter

import (
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestParser(t *testing.T) {
	in1 := `table_1((foo == "val" OR world > 233) AND bar != 123)`
	t.Logf("expression: %v", in1)
	out := split(in1)
	for key, expression := range out {
		key = strings.TrimSpace(key)
		expression = strings.TrimSpace(expression)
		if regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`).MatchString(key) {
			if nodes, err := parse(expression); err != nil {
				t.Error(err)
			} else {
				t.Logf("result node: %#v", nodes)
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

type node string

func (n node) Type() NodeType {
	return NodeType(n)
}

func Test_getMatchedChunk(t *testing.T) {
	type args struct {
		what RuleItem
		in   []Node
	}
	tests := []struct {
		name     string
		args     args
		wantHead []Node
		wantTail []Node
		wantOk   bool
	}{
		{
			name: "empty",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 0,
					MaxCount: -1,
				},
				in: nil,
			},
			wantHead: nil,
			wantTail: nil,
			wantOk:   true,
		},
		{
			name: "one element",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 1,
					MaxCount: 1,
				},
				in: []Node{node("a"), node("b")},
			},
			wantHead: []Node{node("a")},
			wantTail: []Node{node("b")},
			wantOk:   true,
		},
		{
			name: "more elements",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 1,
					MaxCount: -1,
				},
				in: []Node{node("a"), node("a"), node("b")},
			},
			wantHead: []Node{node("a"), node("a")},
			wantTail: []Node{node("b")},
			wantOk:   true,
		},
		{
			name: "more elements from 0",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 0,
					MaxCount: -1,
				},
				in: []Node{node("a"), node("a"), node("b")},
			},
			wantHead: []Node{node("a"), node("a")},
			wantTail: []Node{node("b")},
			wantOk:   true,
		},
		{
			name: "more elements {1:2}",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 1,
					MaxCount: 2,
				},
				in: []Node{node("a"), node("a"), node("a"), node("b")},
			},
			wantHead: []Node{node("a"), node("a")},
			wantTail: []Node{node("a"), node("b")},
			wantOk:   true,
		},
		{
			name: "not match",
			args: args{
				what: RuleItem{
					nodeType: "a",
					MinCount: 3,
					MaxCount: 3,
				},
				in: []Node{node("a"), node("a"), node("b")},
			},
			wantHead: nil,
			wantTail: []Node{node("a"), node("a"), node("b")},
			wantOk:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHead, gotTail, gotOk := getMatchedChunk(tt.args.what, tt.args.in)
			if !reflect.DeepEqual(gotHead, tt.wantHead) {
				t.Errorf("getMatchedChunk() gotHead = %v, want %v", gotHead, tt.wantHead)
			}
			if !reflect.DeepEqual(gotTail, tt.wantTail) {
				t.Errorf("getMatchedChunk() gotTail = %v, want %v", gotTail, tt.wantTail)
			}
			if gotOk != tt.wantOk {
				t.Errorf("getMatchedChunk() gotOk = %v, want %v", gotOk, tt.wantOk)
			}
		})
	}
}
