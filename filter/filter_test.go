package filter

import (
	"testing"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		name         string
		expression   string
		data         map[string]interface{}
		want         bool
		wantParseErr bool
		wantErr      bool
	}{
		{
			name:       "empty expression",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: "",
			want:       true,
		},
		{
			name:       "simple one op with number",
			data:       map[string]interface{}{"foo": 123, "bar": "val2"},
			expression: `foo == 123`,
			want:       true,
		},
		{
			name:       "simple one op with number, >",
			data:       map[string]interface{}{"foo": 123, "bar": "val2"},
			expression: `foo > 120`,
			want:       true,
		},
		{
			name:       "simple one op with float",
			data:       map[string]interface{}{"foo": 123.5, "bar": "val2"},
			expression: `foo > 123.1`,
			want:       true,
		},
		{
			name:       "simple one op with float (false result)",
			data:       map[string]interface{}{"foo": 123.5, "bar": "val2"},
			expression: `foo < 123.1`,
			want:       false,
		},
		{
			name:       "simple one op with string",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: `foo == "val1"`,
			want:       true,
		},
		{
			name:       "simple one op with NOT",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: `NOT (foo == "val1")`,
			want:       false,
		},
		{
			name:       "simple one op with NOT",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: `NOT foo == "val1"`,
			want:       false,
		},
		{
			name:       "IS NULL",
			data:       map[string]interface{}{"foo": nil},
			expression: `foo IS NULL`,
			want:       true,
		},
		{
			name:       "IS NULL - false",
			data:       map[string]interface{}{"foo": "val"},
			expression: `foo IS NULL`,
			want:       false,
		},
		{
			name:       "NOT IS NULL",
			data:       map[string]interface{}{"foo": nil},
			expression: `NOT foo IS NULL`,
			want:       false,
		},
		{
			name:       "simple one op with string, !=",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: `foo == "val another"`,
			want:       false,
		},
		{
			name:       "AND op",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `foo == "val1" AND bar == 123`,
			want:       true,
		},
		{
			name:       "OR op",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `foo == "val1" OR bar == 121`,
			want:       true,
		},
		{
			name:       "IN op with one element",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `bar IN (123)`,
			want:       true,
		},
		{
			name:       "IN op with two element",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `bar IN (123, 456)`,
			want:       true,
		},
		{
			name:       "IN op with three element",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `bar IN (123, 456, "dwed")`,
			want:       true,
		},
		{
			name:       "IN op with three element and NOT",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `NOT bar IN (123, 456, "dwed")`,
			want:       false,
		},
		{
			name:       "IN op with three element and NOT - false",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `NOT bar IN (1232, 456, "dwed")`,
			want:       true,
		},
		{
			name:       "IN op with one element - false",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `bar IN (100)`,
			want:       false,
		},
		{
			name:       "IN op with one element and NOT",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `NOT bar IN (100)`,
			want:       true,
		},
		{
			name:       "OR op with false",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `foo == "val" OR bar != 123`,
			want:       false,
		},
		{
			name:         "not found field",
			data:         map[string]interface{}{"foo": "val1", "bar": 123},
			expression:   `foo == "val" OR bar2 != 123`,
			want:         false,
			wantParseErr: false,
			wantErr:      true,
		},
		{
			name:         "LIKE expr 1",
			data:         map[string]interface{}{"foo": "val1", "bar": 123},
			expression:   `foo LIKE "val1"`,
			want:         true,
			wantParseErr: false,
			wantErr:      false,
		},
		{
			name:         "LIKE expr 2",
			data:         map[string]interface{}{"foo": "val1", "bar": 123},
			expression:   `foo LIKE "val%"`,
			want:         true,
			wantParseErr: false,
			wantErr:      false,
		},
		{
			name:         "LIKE expr 3",
			data:         map[string]interface{}{"foo": "val1", "bar": 123},
			expression:   `foo LIKE "val_"`,
			want:         true,
			wantParseErr: false,
			wantErr:      false,
		},
		{
			name:         "LIKE expr 4",
			data:         map[string]interface{}{"foo": "111val1111", "bar": 123},
			expression:   `foo LIKE "%val%"`,
			want:         true,
			wantParseErr: false,
			wantErr:      false,
		},
		{
			name:         "LIKE expr 5",
			data:         map[string]interface{}{"foo": "111val1111", "bar": 123},
			expression:   `foo LIKE "%v_l%"`,
			want:         true,
			wantParseErr: false,
			wantErr:      false,
		},
		{
			name:         "LIKE expr 6",
			data:         map[string]interface{}{"foo": "val1", "bar": 123},
			expression:   `foo LIKE 123`,
			want:         false,
			wantParseErr: true,
			wantErr:      false,
		},
		{
			name:         "types mismatch",
			data:         map[string]interface{}{"foo": "val", "bar": 123},
			expression:   `foo > 123 OR bar != 123`,
			want:         false,
			wantParseErr: false,
			wantErr:      true,
		},
		{
			name:         "broken expression 1",
			data:         map[string]interface{}{"foo": "val", "bar": 123},
			expression:   `foo > 123 OR 123`,
			want:         false,
			wantParseErr: true,
			wantErr:      false,
		},
		{
			name:         "broken expression 2",
			data:         map[string]interface{}{"foo": "val", "bar": 123},
			expression:   `foo`,
			want:         false,
			wantParseErr: true,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewFilter(tt.expression)
			if (err != nil) != tt.wantParseErr {
				t.Errorf("NewFilter() error = %v, wantErr %v", err, tt.wantParseErr)
				return
			}
			if err != nil {
				return
			}

			want, err := got.Value(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			if want != tt.want {
				t.Errorf("Value() = %v, want %v", want, tt.want)
				return
			}
		})
	}
}
