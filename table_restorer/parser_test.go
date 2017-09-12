package table_restorer

import (
	"os"
	"testing"
)

var lines = []byte(`1050,,29,"yellowbot","2013-04-28 22:47:31",2,"Old proverb says: \"God sends food and the devil sends cooks","Richard A","I had high hopes for this place. The menu looks great and the atmosphere is good. They had a good selection of beer on tap.\nI believe the fish was probably good until the cooks touched it. We each had different fish, Amberjack, Scottish Salmon, Shrimp and Swordfish. All were over cooked, dry and unappetizing. My side of asparagus was...","","b0405f762ccefbc2bcf27b0a8522ea6ee76f5be4","tripadvisor",
`)
var expected = []string{
	`1050`,
	``,
	`29`,
	"yellowbot",
	"2013-04-28 22:47:31",
	"2",
	"Old proverb says: \"God sends food and the devil sends cooks",
	"Richard A",
	"I had high hopes for this place. The menu looks great and the atmosphere is good. They had a good selection of beer on tap.\nI believe the fish was probably good until the cooks touched it. We each had different fish, Amberjack, Scottish Salmon, Shrimp and Swordfish. All were over cooked, dry and unappetizing. My side of asparagus was...",
	"",
	"b0405f762ccefbc2bcf27b0a8522ea6ee76f5be4",
	"tripadvisor  2",
	"",
}

func TestLineParser(t *testing.T) {
	f, _ := os.Open("../test/rf.csv")
	r := NewReader(f)
	c := make(chan []string)
	go r.Parse(c)
	total := 0
	for row := range c {
		total ++
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
