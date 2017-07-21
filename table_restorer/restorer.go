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
	defer close(row)
	for {
		ru, _, err := r.r.ReadRune()
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading: %s", err)
			}
			return
		}
		switch ru {
		case '"':
			column = append(column, ru)
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
					break
				} else {
					column = append(column, ru)
				}
			}
		case '\n':
			columns = append(columns, string(column))
			row <- columns
			column = []rune{}
			columns = []string{}
		case ',':
			columns = append(columns, string(column))
			column = []rune{}
		default:
			column = append(column, ru)
		}
	}
}
