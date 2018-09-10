package filter

import (
	"testing"

	"github.com/pkg/errors"
)

func TestExpr_eval(t *testing.T) {
	tests := []struct {
		name    string
		expr    Expr
		args    map[string]interface{}
		want    bool
		wantErr error
	}{
		{
			name:    "empty expression",
			expr:    Expr{},
			args:    map[string]interface{}{},
			want:    false,
			wantErr: errExpectedExpression,
		},
		{
			name: "without operand",
			expr: Expr{
				Type: OperandExpression,
			},
			args:    map[string]interface{}{},
			want:    false,
			wantErr: errOperandNotFound,
		},
		{
			name: "simple with equal, but without field in data",
			expr: Expr{
				Type: OperandExpression,
				Op:   OpEq,
				X: &Expr{
					Type: OperandField,
					Name: "field01",
				},
				Y: &Expr{
					Type:  OperandValue,
					Value: "val01",
				},
			},
			args:    map[string]interface{}{},
			want:    false,
			wantErr: errFieldNotFound,
		},
		{
			name: "simple with equal",
			expr: Expr{
				Type: OperandExpression,
				Op:   OpEq,
				X: &Expr{
					Type: OperandField,
					Name: "field01",
				},
				Y: &Expr{
					Type:  OperandValue,
					Value: "val01",
				},
			},
			args:    map[string]interface{}{"field01": "val01"},
			want:    true,
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expr := tt.expr
			got, err := expr.eval(tt.args)
			if errors.Cause(err) != tt.wantErr {
				t.Errorf("Expr.eval() error = %v, wantErr %v", errors.Cause(err), tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Expr.eval() = %v, want %v", got, tt.want)
			}
		})
	}
}
