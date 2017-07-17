package mylogin_reader

import (
	"fmt"
	"log"
	"os"

	"github.com/dolmen-go/mylogin"
	"github.com/go-ini/ini"
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
		if *login.Host == "localhost" || *login.Host == "127.0.0.1" {
			if socket := FindSocketFile(); socket != "" {
				login.Socket = &socket
			}
		}
		return login.DSN(), nil
	}
	return "", fmt.Errorf("no sections found")
}

func FindSocketFile() string {
	if cfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, os.Getenv("HOME")+"/.my.cnf"); err == nil {
		if socket, err := cfg.Section("client").GetKey("socket"); err == nil {
			if _, err := os.Stat(socket.String()); !os.IsNotExist(err) {
				return socket.String()
			}
			log.Printf("Socket is specified in ~/.my.cnf but not found in the file system")
		}
	}
	if cfg, err := ini.LoadSources(ini.LoadOptions{AllowBooleanKeys: true}, "/etc/mysql/my.cnf"); err == nil {
		if socket, err := cfg.Section("client").GetKey("socket"); err == nil {
			if _, err := os.Stat(socket.String()); !os.IsNotExist(err) {
				return socket.String()
			}
			log.Printf("Socket is specified in /etc/mysql/my.cnf but not found in the file system")
		}
	}
	return ""
}
