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
			name:       "simple one op with string",
			data:       map[string]interface{}{"foo": "val1", "bar": "val2"},
			expression: `foo == "val1"`,
			want:       true,
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
			name:       "OR op with false",
			data:       map[string]interface{}{"foo": "val1", "bar": 123},
			expression: `foo == "val" OR bar != 123`,
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fields := []string{}
			for key := range tt.data {
				fields = append(fields, key)
			}

			got, err := NewFilter(tt.expression, fields)
			if (err != nil) != tt.wantParseErr {
				t.Errorf("NewFilter() error = %v, wantErr %v", err, tt.wantParseErr)
				return
			}

			want, err := got.Value(tt.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Value() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if want != tt.want {
				t.Errorf("Value() = %v, want %v", want, tt.want)
			}
		})
	}
}
