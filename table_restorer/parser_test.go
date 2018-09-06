package table_restorer

import (
	"bytes"
	"os"
	"testing"
)

var lines = []byte("`field`,`field2`\n" + `1050,,29,"yellowbot\"","2013-04-28 22:47:31\\\\",2,"Old: \"God sends food","Richard A\\","tap.\nI","","b0405f762ccefbc2bcf27b0a8522ea6ee76f5be4","\\\\tripadvisor  2",
`)
var expected = []interface{}{
	float64(1050),           // 0
	nil,                     // 1
	float64(29),             // 2
	"yellowbot\"",           // 3
	`2013-04-28 22:47:31\\`, // 4
	float64(2),              // 5
	`Old: "God sends food`,  // 6
	"Richard A\\",           // 7
	"tap.\nI",               // 8
	"",                      // 9
	"b0405f762ccefbc2bcf27b0a8522ea6ee76f5be4", // 10
	`\\tripadvisor  2`,                         // 11
	nil,
}

func TestLineParser(t *testing.T) {
	r := NewReader(bytes.NewReader(lines))
	c := make(chan []interface{})
	go r.Parse(c)
	total := 0
	for row := range c {
		total++
		if len(row) != 13 {
			t.Logf("got row of %d cols", len(row))
			t.Error()
		} else {
			for i, column := range row {
				//t.Logf("got column %d: %v", i, column)
				if i > 20 {
					t.Fatal()
				}
				if column != expected[i] {
					t.Errorf("Expected (%d) %v, got %v", i, expected[i], column)
				}
			}
		}
	}
	t.Logf("Total %d", total)
}

func TestLineParser2(t *testing.T) {
	f, _ := os.Open("../test/rf.csv")
	r := NewReader(f)
	c := make(chan []interface{})
	go r.Parse(c)
	total := 0
	for row := range c {
		total++
		if len(row) != 13 {
			t.Logf("got row of %d cols", len(row))
			for i, column := range row {
				t.Logf("got column %d: %s", i, column)
				if i > 20 {
					t.Fatal()
				}
				//if column != expected[i] {
				//	t.Errorf("Expected %q, got %q", expected[i], column)
				//}
			}
			t.Error()
		}
	}
	t.Logf("Total %d", total)
}
