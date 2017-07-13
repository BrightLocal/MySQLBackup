package mylogin_reader

import (
	"fmt"
	"os"

	"github.com/dolmen-go/mylogin"
)

type Reader struct {
	fileName string
	client   *mylogin.Login
}

func Read(name ...string) *Reader {
	r := &Reader{}
	if len(name) == 0 {
		home := os.Getenv("HOME")
		myLogin := home + "/.mylogin.cnf"
		my := home + "/.my.cnf"
		if _, err := os.Stat(myLogin); os.IsNotExist(err) {
			if _, err := os.Stat(my); !os.IsNotExist(err) {
				r.fileName = my
			}
		} else {
			r.fileName = myLogin
		}
	} else {
		r.fileName = name[0]
	}
	return r
}

func (r *Reader) GetDSN(login string) (string, error) {
	if r.fileName == "" {
		return "", fmt.Errorf("could not find MySQL credentials file anywhere")
	}
	s, err := mylogin.ReadSections(r.fileName)
	if err != nil {
		return "", err
	}
	if login := s.Login(login); login != nil {
		return login.DSN(), nil
	}
	return "", fmt.Errorf("no sections found")
}
