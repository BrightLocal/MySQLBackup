package dir_dumper

import (
	"compress/gzip"
	"errors"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

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
	fileName := name + ".csjson.gz"
	writer, err := d.getWriter(fileName)
	if err != nil {
		log.Fatalf("Error getting writer: %s", err)
	}
	defer writer.Close()
	gzWriter := gzip.NewWriter(writer)
	defer gzWriter.Close()
	if err := td.Run(gzWriter); err != nil {
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
	var authenticationMethods []ssh.AuthMethod
	if aConn, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK")); err == nil {
		authenticationMethods = append(authenticationMethods, ssh.PublicKeysCallback(agent.NewClient(aConn).Signers))
	}
	conn, err := ssh.Dial("tcp", where.Host, &ssh.ClientConfig{
		User:            where.User.Username(),
		Auth:            authenticationMethods,
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

func (d *DirDumper) getFileWriter(fileName string) (io.WriteCloser, error) {
	return os.Create(d.dir + "/" + fileName)
}
