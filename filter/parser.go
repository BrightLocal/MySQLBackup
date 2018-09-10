package filter

import (
	"log"
	"regexp"
	"strings"
)

var (
	rValidKey = regexp.MustCompile("^[a-zA0-9_]+$")
	rNumbers  = regexp.MustCompile("^[0-9.]+$")
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

func parse(expr string) {
	log.Printf("%s", expr)
	parts := tokenize(expr)
	for _, value := range parts {
		if isSplitter(value) {
			log.Printf("Operator: %q", value)
		} else if isValue(value) {
			log.Printf("Value: %q", value)
		} else {
			log.Printf("Field: %q", value)
		}
	}
}

var splitters = map[string]string{
	"(":       "",
	")":       "",
	",":       "",
	"=":       "",
	"==":      "",
	"!=":      "",
	">":       "",
	">=":      "",
	"<":       "",
	"<=":      "",
	"'":       "",
	"AND":     "",
	"OR":      "",
	"NOT":     "",
	"IS NULL": "",
}

const tokenLen = 7

func isSplitter(value string) bool {
	_, ok := splitters[value]
	return ok
}

func isValue(value string) bool {
	return rNumbers.MatchString(value) || rString.MatchString(value)
}

func tokenize(expr string) []string {
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

func balance(in []string) []string {
	var out []string
	token := ""
	for i := 0; i < len(in); i++ {
		if in[i] == "'" {
			if token == "" {
				token = in[i]
			} else {
				token += in[i]
				out = append(out, token)
				token = ""
			}
		} else {
			if token == "" {
				if t := strings.TrimSpace(in[i]); t != "" {
					out = append(out, t)
				}
			} else {
				token += in[i]
			}
		}
	}
	return out
}
