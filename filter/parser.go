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
	Pattern    []RuleItem
}

type RuleItem struct {
	Type     NodeType
	MinCount int // item with modificators '*','+','?' like "... IN ( Literal* )"
	MaxCount int
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

			argument, err := parseLiteral(string(params[2].(SrcNode)))
			if err != nil {
				return OpError{errorMsg: err.Error()}
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
	{
		Pattern: parsePattern("Field IS_NULL"),
		CreateNode: func(params []Node) Node {
			return OpIsNull{
				field: string(params[0].(SrcNode)),
			}
		},
	},
	{
		Pattern: parsePattern("Field IN ( Literal )"),
		CreateNode: func(params []Node) Node {
			if len(params) < 5 {
				return OpError{errorMsg: fmt.Sprintf("not enough arguments for IN operation: %+v", params)}
			}

			arguments := []interface{}{}
			for _, item := range params[3 : len(params)-1] {
				literal, err := parseLiteral(string(item.(SrcNode)))
				if err != nil {
					return OpError{errorMsg: err.Error()}
				}
				arguments = append(arguments, literal)
			}

			return OpIn{
				field:     string(params[0].(SrcNode)),
				arguments: arguments,
			}
		},
	},
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

		if strings.HasSuffix(item, "+") {
			// TODO handle repeat count
			item = strings.TrimRight(item, "+")
			result = append(result, RuleItem{
				Type: NodeType(item),
			})
		}

		if strings.HasSuffix(item, "*") {
			item = strings.TrimRight(item, "*")
			// TODO handle repeat count
		}

		result = append(result, RuleItem{
			Type: NodeType(item),
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
					if ruleElem.Type != nextToken.Type() {
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
				errors.New("expected field or value or opening brace after " + p)
			}
		case "IS NULL": // "field IS NULL"
			if !isField(tokens[i-1]) {
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

const maxTokenLen = 64 // the longest splitter length

func isSplitter(value string) bool {
	_, ok := splitters[value]
	return ok
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
