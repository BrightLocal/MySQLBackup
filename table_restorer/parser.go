package table_restorer

import (
	"bufio"
	"io"
	"log"
)

type LineReader struct {
	r *bufio.Reader
}

func NewReader(input io.Reader) *LineReader {
	return &LineReader{
		r: bufio.NewReader(input),
	}
}

func (r *LineReader) Parse(row chan []string) {
	columns := []string{}
	column := []rune{}
	defer func() {
		log.Printf("Closing rows")
		close(row)
	}()
	escaped := false
	for {
		ru, _, err := r.r.ReadRune()
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading: %s", err)
			}
			return
		}
		switch ru {
		case '"': // beginning a string in ""
			column = append(column, ru)
			if !escaped { // not escaped: \"
				for {
					ru, _, err := r.r.ReadRune()
					if err != nil {
						if err != io.EOF {
							log.Printf("Error reading: %s", err)
						}
						return
					}
					if ru == '"' {
						column = append(column, ru)
						if !(len(column) > 1 && column[len(column)-2] == '\\') {
							break
						}
					} else {
						column = append(column, ru)
					}
				}
			}
			escaped = false
		case '\\': // escaped character
			column = append(column, ru)
			escaped = !escaped
		case '\n': // end of line
			columns = append(columns, string(column))
			row <- columns
			column = []rune{}
			columns = []string{}
			escaped = false
		case ',': // new column
			columns = append(columns, string(column))
			column = []rune{}
			escaped = false
		default: // just a character
			column = append(column, ru)
			escaped = false
		}
	}
}
