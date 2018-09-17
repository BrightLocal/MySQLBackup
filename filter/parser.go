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
	CreateNode func(params []Node) Node
	Pattern    []NodeType
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
		Pattern: parsePattern("Field SimpleOp Literal"),
		CreateNode: func(params []Node) Node {
			field := string(params[0].(SrcNode))

			argumentStr := string(params[2].(SrcNode))
			var argument interface{}

			if reString.MatchString(argumentStr) {
				argument = argumentStr[1 : len(argumentStr)-1]
			} else if parsedInt64, err := strconv.ParseInt(argumentStr, 10, 64); err == nil {
				argument = int(parsedInt64)
			} else if parsedFloat64, err := strconv.ParseFloat(argumentStr, 64); err == nil {
				argument = parsedFloat64
			} else {
				return OpError{errorMsg: fmt.Sprintf("failed to parse literal: %v", argumentStr)}
			}

			switch params[1].(SrcNode) {
			case "==":
				return OpEq{field: field, argument: argument}
			case "!=":
				return OpNe{field: field, argument: argument}
			case ">":
				return OpGt{field: field, argument: argument}
			case ">=":
				return OpGe{field: field, argument: argument}
			case "<":
				return OpLt{field: field, argument: argument}
			case "<=":
				return OpLe{field: field, argument: argument}
			default:
				return OpError{errorMsg: fmt.Sprintf("not known operation: %v", params[1])}
			}
		},
	},
	// TODO: IS NULL, IN ()
	{
		Pattern: parsePattern("NOT BoolExpr"),
		CreateNode: func(params []Node) Node {
			return OpNot{
				x: params[1].(BoolExpr),
			}
		},
	},
	{
		Pattern: parsePattern("BoolExpr AND BoolExpr"),
		CreateNode: func(params []Node) Node {
			return OpAnd{
				x: params[0].(BoolExpr),
				y: params[2].(BoolExpr),
			}
		},
	},
	{
		Pattern: parsePattern("BoolExpr OR BoolExpr"),
		CreateNode: func(params []Node) Node {
			return OpOr{
				x: params[0].(BoolExpr),
				y: params[2].(BoolExpr),
			}
		},
	},
	{
		Pattern: parsePattern("( BoolExpr )"),
		CreateNode: func(params []Node) Node {
			return params[1]
		},
	},
}

func parsePattern(pattern string) []NodeType {
	strPattern := reSplitBySpaces.Split(pattern, -1)
	result := make([]NodeType, 0, len(strPattern))
	for _, item := range strPattern {
		result = append(result, NodeType(item))
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

func parse(expr string, fields []string) (BoolExpr, error) {
	rawTokens := tokenize(expr, fields)
	if err := validate(rawTokens, fields); err != nil {
		return nil, err
	}
	if len(rawTokens) == 0 {
		return OpNop{}, nil
	}

	tokens := make([]Node, 0, len(rawTokens))
	for _, item := range rawTokens {
		tokens = append(tokens, SrcNode(item))
	}

	applyRulesCount := 1
	for applyRulesCount > 0 {
		applyRulesCount = 0

		for _, rule := range rules {
			if len(rule.Pattern) > len(tokens) {
				continue
			}

			newTokens := []Node{}
			i := 0

		ShiftRigth:
			for i < len(tokens) { // iterate by tokens for each rule
				params := []Node{}
				for _, ruleElem := range rule.Pattern {
					if i == len(tokens) {
						// rule length > tail of tokens, pass rest of tokens as is
						newTokens = append(newTokens, params...)
						continue ShiftRigth
					}

					nextToken := tokens[i]
					params = append(params, nextToken)
					i++
					if ruleElem != nextToken.Type() {
						// pattern NOT match or tokens is finished, pass old tokens
						newTokens = append(newTokens, params...)
						continue ShiftRigth
					}
				}
				// pattern match! replace by new Node
				applyRulesCount++
				newTokens = append(newTokens, rule.CreateNode(params))
			}

			tokens = newTokens
		} // rules
	}

	if len(tokens) != 1 {
		return nil, errors.Errorf("there were not recognized tokens: %#v", tokens)
	}
	if tokens[0].Type() != "BoolExpr" {
		return nil, errors.Errorf("result operation isn't bool expression: %+v", tokens[0].Type())
	}

	return tokens[0].(BoolExpr), nil
}

// validates the tokenized expression
func validate(tokens, fields []string) error {
	bracesCount := 0
	for i, p := range tokens {
		if !(isSplitter(p) || isValue(p) || isField(p, fields)) {
			return errors.New(fmt.Sprintf("unknown token %q at position %d", p, i))
		}
		switch p {
		case "(": // "( field", "( value"
			bracesCount++
			if i == len(tokens)-1 || !(isField(tokens[i+1], fields) || isValue(tokens[i+1])) {
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
			if !(tokens[i+1] == "(" || isValue(tokens[i+1]) || isField(tokens[i+1], fields)) {
				errors.New("expected field or value or opening brace after " + p)
			}
		case "IS NULL": // "field IS NULL"
			if !isField(tokens[i-1], fields) {
				errors.New("expected field before IS NULL")
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
	splitters = map[string]string{
		"(":       "",
		")":       "",
		",":       "",
		"'":       "",
		"==":      "",
		"!=":      "",
		">":       "",
		">=":      "",
		"<":       "",
		"<=":      "",
		"IN":      "",
		"AND":     "",
		"OR":      "",
		"NOT":     "",
		"IS NULL": "",
	}
)

const maxTokenLen = 7 // the longest splitter length

func isSplitter(value string) bool {
	_, ok := splitters[value]
	return ok
}

func isValue(value string) bool {
	return reNumbers.MatchString(value) || reString.MatchString(value)
}

func isField(value string, fields []string) bool {
	for _, f := range fields {
		if value == f {
			return true
		}
	}
	return false
}

func tokenize(expr string, fields []string) []string {
	tokenLen := maxTokenLen
	f := make(map[string]string, len(fields))
	for _, field := range fields {
		f[field] = "field"
		if l := len(field); l > maxTokenLen {
			tokenLen = l
		}
	}
	l := len(expr)
	pos := 0
	token := ""
	var tokens []string
a:
	for pos < l {
		for i := tokenLen; i > 0; i-- {
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
