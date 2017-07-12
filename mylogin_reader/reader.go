package mylogin_reader

import (
	"fmt"
	"os"

	"github.com/dolmen-go/mylogin"
)

type Reader struct {
	fileName string
}

func Read(name ...string) *Reader {
	var fileName string
	if len(name) == 0 {
		home := os.Getenv("HOME")
		myLogin := home + "/.mylogin.cnf"
		my := home + "/.my.cnf"
		if _, err := os.Stat(myLogin); os.IsNotExist(err) {
			if _, err := os.Stat(my); !os.IsNotExist(err) {
				fileName = my
			}
		} else {
			fileName = myLogin
		}
	} else {
		fileName = name[0]
	}
	return &Reader{
		fileName: fileName,
	}
}

func (r *Reader) GetDSN() (string, error) {
	s, err := mylogin.ReadSections(r.fileName)
	if err != nil {
		return "", err
	}
	if len(s) > 0 {
		dsn := fmt.Sprintf(
			"%s:%s@tcp(%s:%s)",
			*(s[0].Login.User),
			*(s[0].Login.Password),
			*(s[0].Login.Host),
			*(s[0].Login.Port),
		)
		return dsn, nil
	}
	return "", fmt.Errorf("no sections found")
}
