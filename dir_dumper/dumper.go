package dir_dumper

import (
	"errors"
	"io"
	"log"
	"net/url"
	"strings"

	"net"
	"os"

	"github.com/BrightLocal/MySQLBackup/table_dumper"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type DirDumper struct {
	dsn    string
	dir    string
	config table_dumper.Config
}

func NewDirDumper(dsn, dir string, config table_dumper.Config) *DirDumper {
	return &DirDumper{
		dsn:    dsn,
		dir:    dir,
		config: config,
	}
}

func (d *DirDumper) Dump(tableName interface{}) {
	name := tableName.(string)
	td := table_dumper.NewTableDumper(d.dsn, name, d.config)
	fileName := name + ".json.gz"
	writer, err := d.getWriter(fileName)
	if err != nil {
		log.Fatalf("Error getting writer: %s", err)
	}
	defer writer.Close()
	if err := td.Run(writer); err != nil {
		log.Printf("Error running worker: %s", err)
	}
}

func (d *DirDumper) getWriter(fileName string) (io.WriteCloser, error) {
	if strings.HasPrefix(d.dir, "sftp://") {
		where, err := url.Parse(d.dir)
		if err != nil {
			return nil, err
		}
		if where.User == nil {
			return nil, errors.New("user name expected")
		}
		if where.User.Username() == "" {
			return nil, errors.New("user name expected")
		}
		if where.Path == "" {
			return nil, errors.New("path expected")
		}
		if where.Host == "" {
			return nil, errors.New("host name is empty expected")
		}
		if where.Port() == "" {
			where.Host = where.Host + ":22"
		}
		return d.getSFTPWriter(fileName, where)
	}
	return d.getFileWriter(fileName)
}

func (d *DirDumper) getSFTPWriter(fileName string, where *url.URL) (io.WriteCloser, error) {
	var auths []ssh.AuthMethod
	if aConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		auths = append(auths, ssh.PublicKeysCallback(agent.NewClient(aConn).Signers))
	}
	conn, err := ssh.Dial("tcp", where.Host, &ssh.ClientConfig{
		User:            where.User.Username(),
		Auth:            auths,
		HostKeyCallback: nil,
	})
	if err != nil {
		return nil, err
	}
	f, err := sftp.NewClient(conn)
	if err != nil {
		return nil, err
	}
	return f.Create(where.Path + "/" + fileName)
}

func (d *DirDumper) getFileWriter(tableName string) (io.WriteCloser, error) {
	return nil, nil
}
