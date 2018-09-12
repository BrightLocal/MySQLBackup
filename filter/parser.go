package filter

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type OperatorStub struct{}

func (OperatorStub) Value(values map[string]interface{}) (bool, error) {
	return false, nil
}

var (
	rValidKey = regexp.MustCompile("^[a-zA0-9_]+$")
	rNumbers  = regexp.MustCompile("^-?[0-9.]+$")
	rString   = regexp.MustCompile("^('.*')|(\".*\")$")
)

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

func parse(expr string, fields []string) (Operator, error) {
	log.Printf("Source: %s", expr)
	tokens := tokenize(expr, fields)
	if err := validate(tokens, fields); err != nil {
		return nil, err
	}
	for i, value := range tokens {
		if isSplitter(value) {
			buildNode(i, tokens)
			// TODO recursively build nodes tree
			//return buildNode(i, tokens), nil
		}
	}
	return nil, errors.New("no operators defined")
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

// extracts part to the left of i until the unpaired "(" or the beginning
func extractLeft(i int, tokens []string) []string {
	var left []string
	for j := i - 1; j >= 0; j-- {
		if tokens[j] == ")" {
			for {
				if tokens[j] == "(" {
					j--
					break
				}
				j--
			}
		}
		if tokens[j] == "(" || j == 0 {
			left = tokens[j:i]
			break
		}
	}
	return left
}

// extracts part to the right or i until the unpaired ")" or the ending
func extractRight(i int, tokens []string) []string {
	var right []string
	for j := i + 1; j < len(tokens); j++ {
		if tokens[j] == "(" {
			for {
				if tokens[j] == ")" {
					j++
					break
				}
				j++
			}
		}
		if tokens[j] == ")" || j == len(tokens)-1 {
			right = tokens[i+1 : j+1]
			break
		}
	}
	return right
}

// recursively build operators tree
func buildNode(i int, tokens []string) Operator {
	switch tokens[i] {
	case "==":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("EQ: %s == %s", arg1, arg2)
	case "!=":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("NE: %s != %s", arg1, arg2)
	case ">":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("GT: %s > %s", arg1, arg2)
	case "<":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("LT: %s < %s", arg1, arg2)
	case ">=":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("GE: %s >= %s", arg1, arg2)
	case "<=":
		arg1, arg2 := tokens[i-1], tokens[i+1]
		// TODO
		log.Printf("LE: %s <= %s", arg1, arg2)
	case "AND":
		left, right := extractLeft(i, tokens), extractRight(i, tokens)
		// TODO
		log.Printf("AND: %v AND %v", left, right)
	case "OR":
		left, right := extractLeft(i, tokens), extractRight(i, tokens)
		// TODO
		log.Printf("OR: %v OR %v", left, right)
	case "IN":
		arg1 := tokens[i-1]
		var arg2 []string
		for j := i + 1; j < len(tokens); j++ {
			if tokens[j] == ")" {
				arg2 = tokens[i+2 : j]
				break
			}
		}
		// TODO
		log.Printf("IN: %s IN %v", arg1, arg2)
	case "IS NULL":
		arg := tokens[i-1]
		// TODO
		log.Printf("IS NULL: %s", arg)
	case "(": // do nothing
	case ")": // do nothing
	case ",": // do nothing
	default:
		log.Fatalf("Unknown operator %q at position %d", tokens[i], i)
	}
	return OperatorStub{} // TODO
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
	return rNumbers.MatchString(value) || rString.MatchString(value)
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
