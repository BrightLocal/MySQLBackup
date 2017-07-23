package table_restorer

import (
	"bytes"
	"encoding/json"
	"testing"
)

var (
	lines = []byte(`74,-12.345,  ,"field with \"escaped\" text","","multi\nline\twith tabs",b92a6b37b33644cd0f39b24efd8b1d038794b62a,,"2011-08-24 12:36:42",`)
	expected = []string{
		`74`,
		`-12.345`,
		`  `,
		`field with "escaped" text`,
		``,
		`multi
line	with tabs`,
		`b92a6b37b33644cd0f39b24efd8b1d038794b62a`,
		``,
		`2011-08-24 12:36:42`,
		``,
	}
)

func TestLineParser(t *testing.T) {
	r := NewReader(bytes.NewReader(lines))
	c := make(chan []string)
	go r.Parse(c)
	i := 0
	for row := range c {
		for _, column := range row {
			if len(column) > 0 && string(column[0]) == "\"" {
				if err := json.Unmarshal([]byte(column), &column); err != nil {
					t.Errorf("Error unmarshaling: %s", err)
				}
			}
			if column != expected[i] {
				t.Errorf("Expected %q, got %q", expected[i], column)
			}
			i++
		}
	}
}
