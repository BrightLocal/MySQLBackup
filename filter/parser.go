package filter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type NodeType string

type Node interface {
	Type() NodeType
}

type BoolExpr interface {
	Node
	Value(data map[string]interface{}) (bool, error)
}

type Rule struct {
	createNodeFn func(params []Node) (Node, error)
	pattern      []RuleItem
}

type RuleItem struct {
	nodeType NodeType
	minCount int // item with modificators '*','+','?' like "... IN ( Literal* )"
	maxCount int
}

func (ri RuleItem) Type() NodeType {
	return ri.nodeType
}

type SrcNode string

func (sn SrcNode) isSimpleOp() bool {
	switch sn {
	case "==", "!=", ">", ">=", "<", "<=":
		return true
	default:
		return false
	}
}

func (sn SrcNode) Type() NodeType {
	switch {
	case sn == "AND":
		return "AND"
	case sn == "OR":
		return "OR"
	case sn == "NOT":
		return "NOT"
	case sn == "IN":
		return "IN"
	case sn == "IS NULL":
		return "IS_NULL"
	case sn == "LIKE":
		return "LIKE"
	case sn.isSimpleOp():
		return "SimpleOp"
	case reField.MatchString(string(sn)):
		return "Field"
	case isValue(string(sn)):
		return "Literal"
	case sn == "(" || sn == ")":
		return NodeType(sn)
	default:
		return "Unknown"
	}
}

var (
	reField         = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]*$`)
	reNumbers       = regexp.MustCompile("^-?[0-9.]+$")
	reString        = regexp.MustCompile("^('.*')|(\".*\")$")
	reSplitBySpaces = regexp.MustCompile(`\s+`)
)

var rules = []Rule{
	{
		pattern: parsePattern("Field SimpleOp Literal"),
		createNodeFn: func(params []Node) (Node, error) {
			field := string(params[0].(SrcNode))

			argument, err := parseLiteral(string(params[2].(SrcNode)))
			if err != nil {
				return nil, err
			}

			switch params[1].(SrcNode) {
			case "==":
				return OpEq{field: field, argument: argument}, nil
			case "!=":
				return OpNe{field: field, argument: argument}, nil
			case ">":
				return OpGt{field: field, argument: argument}, nil
			case ">=":
				return OpGe{field: field, argument: argument}, nil
			case "<":
				return OpLt{field: field, argument: argument}, nil
			case "<=":
				return OpLe{field: field, argument: argument}, nil
			default:
				return nil, errors.Errorf("not known operation: %v", params[1])
			}
		},
	},
	{
		pattern: parsePattern("Field IS_NULL"),
		createNodeFn: func(params []Node) (Node, error) {
			return OpIsNull{
				field: string(params[0].(SrcNode)),
			}, nil
		},
	},
	{
		pattern: parsePattern("Field LIKE Literal"),
		createNodeFn: func(params []Node) (Node, error) {
			reSrc, err := parseLiteral(string(params[2].(SrcNode)))
			if err != nil {
				return nil, errors.Wrapf(err, "failed to parse regexp literal %v: %s", params[2])
			}
			re, ok := reSrc.(string)
			if !ok {
				return nil, errors.Errorf("regexp: %[1]v (%[1]T) must have a string type", reSrc)
			}

			return NewOpLike(string(params[0].(SrcNode)), re)
		},
	},
	{
		pattern: parsePattern("Field IN ( Literal+ )"),
		createNodeFn: func(params []Node) (Node, error) {
			if len(params) < 5 {
				return nil, errors.Errorf("not enough arguments for IN operation: %+v", params)
			}

			arguments := []interface{}{}
			for _, item := range params[3 : len(params)-1] {
				literal, err := parseLiteral(string(item.(SrcNode)))
				if err != nil {
					return nil, err
				}
				arguments = append(arguments, literal)
			}

			return OpIn{
				field:     string(params[0].(SrcNode)),
				arguments: arguments,
			}, nil
		},
	},
	{
		pattern: parsePattern("NOT BoolExpr"),
		createNodeFn: func(params []Node) (Node, error) {
			return OpNot{
				x: params[1].(BoolExpr),
			}, nil
		},
	},
	{
		pattern: parsePattern("BoolExpr AND BoolExpr"),
		createNodeFn: func(params []Node) (Node, error) {
			return OpAnd{
				x: params[0].(BoolExpr),
				y: params[2].(BoolExpr),
			}, nil
		},
	},
	{
		pattern: parsePattern("BoolExpr OR BoolExpr"),
		createNodeFn: func(params []Node) (Node, error) {
			return OpOr{
				x: params[0].(BoolExpr),
				y: params[2].(BoolExpr),
			}, nil
		},
	},
	{
		pattern: parsePattern("( BoolExpr )"),
		createNodeFn: func(params []Node) (Node, error) {
			return params[1], nil
		},
	},
}

func parseLiteral(in string) (interface{}, error) {
	var result interface{}

	if reString.MatchString(in) {
		result = in[1 : len(in)-1]
	} else if parsedInt64, err := strconv.ParseInt(in, 10, 64); err == nil {
		result = int(parsedInt64)
	} else if parsedFloat64, err := strconv.ParseFloat(in, 64); err == nil {
		result = parsedFloat64
	} else {
		return nil, errors.Errorf("failed to parse literal: %v", in)
	}

	return result, nil
}

func parsePattern(pattern string) []RuleItem {
	strPattern := reSplitBySpaces.Split(pattern, -1)
	result := make([]RuleItem, 0, len(strPattern))
	for _, item := range strPattern {
		minCount, maxCount := 1, 1

		if strings.HasSuffix(item, "+") {
			item = strings.TrimRight(item, "+")
			minCount, maxCount = 1, -1
		}

		if strings.HasSuffix(item, "*") {
			item = strings.TrimRight(item, "*")
			minCount, maxCount = 0, -1
		}

		if strings.HasSuffix(item, "?") {
			item = strings.TrimRight(item, "?")
			minCount, maxCount = 0, 1
		}

		result = append(result, RuleItem{
			nodeType: NodeType(item),
			minCount: minCount,
			maxCount: maxCount,
		})
	}

	return result
}

func split(in string) map[string]string {
	result := make(map[string]string)
	key := ""
	expression := ""
	for i := 0; i < len(in); i++ {
		switch {
		case in[i] == '(':
			c := 0
			i++
			for j := i; j < len(in); j++ {
				if in[j] == '(' {
					c++
				} else if in[j] == ')' {
					c--
				}
				i++
				if in[j] == ')' && c == -1 {
					result[key] = expression
					key = ""
					expression = ""
					break
				}
				expression += string(in[j])
			}
		case in[i] == ',':
			key = ""
			expression = ""
		default:
			key += string(in[i])
		}
	}
	return result
}

// maxCount == -1 is infinity
func getMatchedChunk(what RuleItem, in []Node) (head []Node, tail []Node, ok bool) {
	ok = false

	i := 0
	for i < len(in) && what.Type() == in[i].Type() {
		head = append(head, in[i])

		if i+1 >= what.minCount {
			ok = true
		}
		if what.maxCount != -1 && i+1 >= what.maxCount {
			break
		}
		i++
	}

	if !ok && what.minCount == 0 {
		return nil, in, true
	} else if !ok {
		return nil, in, false
	}

	return head, in[len(head):], true
}

func parse(expr string) (BoolExpr, error) {
	rawTokens := tokenize(expr)
	if err := validate(rawTokens); err != nil {
		return nil, err
	}
	if len(rawTokens) == 0 {
		return OpNop{}, nil
	}

	tokens := make([]Node, 0, len(rawTokens))
	for _, item := range rawTokens {
		if item == "," {
			continue
		}
		tokens = append(tokens, SrcNode(item))
	}

	applyRulesCount := 1
	for applyRulesCount > 0 {
		applyRulesCount = 0

		for _, rule := range rules {
			if len(rule.pattern) > len(tokens) {
				continue
			}

			newTokens := []Node{}

		ShiftRigth:
			for len(tokens) > 0 {
				params := []Node{}
				for _, ruleElem := range rule.pattern {
					head, tail, ok := getMatchedChunk(ruleElem, tokens)
					if !ok {
						newTokens = append(newTokens, params...)
						if len(tokens) > 0 {
							newTokens = append(newTokens, tokens[0])
							tokens = tokens[1:]
						}
						continue ShiftRigth
					}

					params = append(params, head...)
					tokens = tail
				}
				// pattern match! replace by new Node
				applyRulesCount++
				newNode, err := rule.createNodeFn(params)
				if err != nil {
					return nil, err
				}
				newTokens = append(newTokens, newNode)
			}

			tokens = newTokens
		} // rules
	}

	if len(tokens) != 1 {
		return nil, errors.Errorf("there were not recognized tokens: %#v", tokens)
	}

	if boolExpr, ok := tokens[0].(BoolExpr); !ok {
		return nil, errors.Errorf("result operation isn't bool expression: %+v", tokens[0].Type())
	} else {
		return boolExpr, nil
	}
}

// validates the tokenized expression
func validate(tokens []string) error {
	bracesCount := 0
	for i, p := range tokens {
		if !(isSplitter(p) || isValue(p) || isField(p)) {
			return errors.New(fmt.Sprintf("unknown token %q at position %d", p, i))
		}
		switch p {
		case "(": // "( field", "( value"
			bracesCount++
			if i == len(tokens)-1 || !(isField(tokens[i+1]) || isValue(tokens[i+1])) {
				return errors.New("expected field or value after opening brace")
			}
			continue
		case ")": // ") AND", ") OR"
			bracesCount--
			if i != len(tokens)-1 {
				if !(tokens[i+1] == "OR" || tokens[i+1] == "AND") {
					return errors.New("expected operator after closing brace")
				}
			}
			continue
		case "IN": // "IN (value,value,...)"
			if tokens[i+1] != "(" {
				return errors.New("opening brace expected after IN")
			}
			odd := true
			closed := false
			for j := i + 2; j < len(tokens); j++ {
				if odd {
					if !isValue(tokens[j]) {
						return errors.New("value expected instead of " + tokens[j])
					}
				} else {
					if !(tokens[j] == "," || tokens[j] == ")") {
						return errors.New("comma or closing brace expected")
					}
				}
				if tokens[j] == ")" {
					i = j + 1
					closed = true
					break
				}
				odd = !odd
			}
			if !closed {
				return errors.New("unexpected end of IN")
			}
		case "OR": // "OR field", "OR value", "OR ("
			fallthrough
		case "AND": // "AND field", "AND value", "AND ("
			if i == len(tokens)-1 {
				return errors.New("unexpected end of " + p)
			}
			if !(tokens[i+1] == "(" || isValue(tokens[i+1]) || isField(tokens[i+1])) {
				return errors.New("expected field or value or opening brace after " + p)
			}
		case "IS NULL": // "field IS NULL"
			if !isField(tokens[i-1]) {
				return errors.New("expected field before IS NULL")
			}
		case "LIKE": // "field LIKE '%x_x_x%'"
			if !isField(tokens[i-1]) {
				return errors.New("expected field before LIKE")
			}
		}
	}
	if bracesCount > 0 {
		return errors.New("unbalanced braces, missing closing brace")
	} else if bracesCount < 0 {
		return errors.New("unbalanced braces, extra closing brace")
	}
	return nil
}

var (
	splitters = map[string]bool{
		"(":       true,
		")":       true,
		",":       true,
		"'":       true,
		"==":      true,
		"!=":      true,
		">":       true,
		">=":      true,
		"<":       true,
		"<=":      true,
		"IN":      true,
		"AND":     true,
		"OR":      true,
		"NOT":     true,
		"IS NULL": true,
		"LIKE":    true,
	}
)

const maxTokenLen = 64 // the longest splitter length

func isSplitter(value string) bool {
	return splitters[value]
}

func isValue(value string) bool {
	return reNumbers.MatchString(value) || reString.MatchString(value)
}

func isField(value string) bool {
	return reField.MatchString(value)
}

func tokenize(expr string) []string {
	l := len(expr)
	pos := 0
	token := ""
	var tokens []string
a:
	for pos < l {
		for i := maxTokenLen; i > 0; i-- {
			s := substr(&expr, pos, i)
			if isSplitter(s) {
				if token != "" {
					tokens = append(tokens, token)
				}
				tokens = append(tokens, s)
				pos += i
				token = ""
				continue a
			}
		}
		token += string(expr[pos])
		pos++
	}
	if token != "" {
		tokens = append(tokens, token)
	}
	return balance(tokens)
}

func substr(expr *string, start, l int) string {
	if len(*expr) > start+l {
		return (*expr)[start : start+l]
	}
	return (*expr)[start:]
}

func balance(tokens []string) []string {
	var out []string
	token := ""
	for i := 0; i < len(tokens); i++ {
		if tokens[i] == "'" {
			if token == "" {
				token = tokens[i]
			} else if token[len(token)-1] == '\\' {
				token += tokens[i]
			} else {
				token += tokens[i]
				out = append(out, token)
				token = ""
			}
		} else {
			if token == "" {
				if t := strings.TrimSpace(tokens[i]); t != "" {
					out = append(out, t)
				}
			} else {
				token += tokens[i]
			}
		}
	}
	return out
}
