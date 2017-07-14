package dir_dumper

import (
	"errors"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/table_dumper"
	"github.com/dsnet/compress/bzip2"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type DumpResult interface {
	Rows() int
	Bytes() int
	Duration() time.Duration
}

type DirDumper struct {
	dsn           string
	dir           string
	config        table_dumper.Config
	totalRows     int
	totalBytes    int
	totalDuration time.Duration
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
	fileName := name + ".csjson.bz2" // comma separated JSON values, compressed
	writer, err := d.getWriter(fileName)
	if err != nil {
		log.Fatalf("Error getting writer: %s", err)
	}
	compressor, _ := bzip2.NewWriter(writer, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	dumpResult, err := td.Run(compressor)
	if err != nil {
		log.Printf("Error running worker: %s", err)
	}
	d.totalBytes += dumpResult.Bytes()
	d.totalRows += dumpResult.Rows()
	d.totalDuration += dumpResult.Duration()
	if err := compressor.Close(); err != nil {
		log.Printf("Error closing compressor: %s", err)
	}
	if err := writer.Close(); err != nil {
		log.Printf("Error closing file %q: %s", fileName, err)
	}
}

func (d *DirDumper) getWriter(fileName string) (io.WriteCloser, error) {
	if strings.HasPrefix(d.dir, "sftp://") {
		where, err := url.Parse(d.dir)
		if err != nil {
			return nil, err
		}
		if where.User == nil || where.User.Username() == "" {
			// Try to figure out user name
			if userName := os.Getenv("USER"); userName != "" {
				where.User = url.UserPassword(userName, "")
			} else {
				if currentUser, err := user.Current(); err == nil {
					where.User = url.UserPassword(currentUser.Username, "")
				} else {
					return nil, errors.New("user name expected")
				}
			}
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
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
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

func (d DirDumper) PrintStats(streams int, totalDuration time.Duration) {
	log.Printf("Dumped %d rows (%d bytes) using %d streams in %s (total run time %s)", d.totalRows, d.totalBytes, streams, d.totalDuration, totalDuration)
}
