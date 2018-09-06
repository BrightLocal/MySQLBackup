package table_restorer

import "strings"

func Builder(in chan []string, out chan string) {
	for row := range in {
		out <- "(" + strings.Join(row, ", ") + ")"
	}
}
