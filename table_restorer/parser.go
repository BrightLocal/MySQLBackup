package table_restorer

import (
	"bufio"
	"encoding/json"
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

func (r *LineReader) Parse(row chan []interface{}) {
	columns := []interface{}{}
	column := []rune{}
	defer func() {
		close(row)
	}()
	escaped := false

	// skip header in ``
	firstRune, _, err := r.r.ReadRune()
	if err != nil {
		if err != io.EOF {
			log.Printf("Error reading: %s", err)
		}
		return
	}
	if firstRune == '`' {
		if _, err := r.r.ReadString('\n'); err != nil {
			if err != io.EOF {
				log.Printf("Error reading: %s", err)
			}
			return
		}
	} else {
		if err := r.r.UnreadRune(); err != nil {
			if err != io.EOF {
				log.Printf("Error reading: %s", err)
			}
			return
		}
	}

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
				//log.Printf("> %s %v", string(ru), escaped)
				if ru == '"' {
					column = append(column, ru)
					if !escaped {
						escaped = false
						break
					}
					escaped = !escaped
				} else if ru == '\\' {
					column = append(column, ru)
					escaped = !escaped
				} else {
					column = append(column, ru)
					escaped = false
				}
			}
			escaped = false
		case '\\': // escaped character
			column = append(column, ru)
			escaped = !escaped
		case '\n': // end of line
			columns = append(columns, parseColumn(column))
			row <- columns
			column = []rune{}
			columns = []interface{}{}
			escaped = false
		case ',': // new column
			columns = append(columns, parseColumn(column))
			column = []rune{}
			escaped = false
		default: // just a character
			column = append(column, ru)
			escaped = false
		}
	}
}

func parseColumn(in []rune) interface{} {
	if len(in) == 0 {
		return nil
	}
	var value interface{}
	if err := json.Unmarshal([]byte(string(in)), &value); err != nil {
		log.Fatalf("error unmarshalling %s: %s", string(in), err)
	}
	return value
}
