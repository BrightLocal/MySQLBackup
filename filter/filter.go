package filter

import (
	"go/ast"
	"go/parser"
	"regexp"
)

type FilterSet map[string]*Filter

type Filter struct {
	tableName string
	ast       ast.Expr
}

// NewFilterSet returns new filters for expression:
// table_name(field == "val"),table02(field02 != "val2" && field03 == 123)
func NewFilterSet(expression string) (FilterSet, error) {
	result := map[string]*Filter{}

	expressionTables := regexp.MustCompile(`\s*,\s*`).Split(expression, -1)
	for _, item := range expressionTables {
		filter, err := NewFilter(item)
		if err != nil {
			return nil, err
		}
		result[filter.tableName] = filter
	}

	return result, nil
}

// NewFilter returns new filter for expression:
// table_name(field == "val")
func NewFilter(expression string) (*Filter, error) {
	result := &Filter{}

	astExpr, err := parser.ParseExpr(expression)
	if err != nil {
		return nil, err
	}
	result.tableName = "flags" // TODO: get name from ast tree
	result.ast = astExpr

	// switch x := astExpr.(type) {
	// case *ast.CallExpr:
	// 	s = x.Fun
	// case *ast.Ident:
	// 	s = x.Name
	// default:
	//    return error
	// }

	return result, nil
}

func (f *Filter) Passes(data map[string]interface{}) bool {
	// TODO: apply expression from ast tree
	return true
}

/*
example for expression:

  proxies(address == "127.0.0.1:9988")

&ast.CallExpr{
  Fun: &ast.Ident{
    NamePos: 1,
    Name:    "proxies",
    Obj:     &ast.Object{
      Kind: 0,
      Name: "",
      Decl: nil,
      Data: nil,
      Type: nil,
    },
  },
  Lparen: 8,
  Args:   []ast.Expr{
    &ast.BinaryExpr{
      X: &ast.Ident{
        NamePos: 9,
        Name:    "address",
        Obj:     &ast.Object{...},
      },
      OpPos: 17,
      Op:    39,
      Y:     &ast.BasicLit{
        ValuePos: 20,
        Kind:     9,
        Value:    "\"127.0.0.1:9988\"",
      },
    },
  },
  Ellipsis: 0,
  Rparen:   36,
}*/
