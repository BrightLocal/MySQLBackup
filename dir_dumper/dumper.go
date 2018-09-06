package dir_dumper

import (
	"errors"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/table_dumper"
	"github.com/dsnet/compress/bzip2"
	"github.com/jmoiron/sqlx"
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
	conn          *sqlx.DB
	totalRows     int
	totalBytes    int
	totalDuration time.Duration
	runAfter      string
	withHeader    bool
}

const fileSuffix = ".csjson.bz2"

func NewDirDumper(dir string, config table_dumper.Config) *DirDumper {
	return &DirDumper{
		dir:    dir,
		config: config,
	}
}

func (d *DirDumper) WithHeader(withHeader bool) *DirDumper {
	d.withHeader = withHeader
	return d
}

func (d *DirDumper) RunAfter(cmd string) *DirDumper {
	d.runAfter = cmd
	return d
}

func (d *DirDumper) Connect(dsn string) *DirDumper {
	d.dsn = dsn
	var err error
	d.conn, err = sqlx.Connect("mysql", d.dsn)
	if err != nil {
		log.Fatalf("Error connecting: %s", err)
	}
	_, err = d.conn.Exec("START TRANSACTION WITH CONSISTENT SNAPSHOT")
	if err != nil {
		log.Fatalf("Error starting transaction: %s", err)
	}
	if d.config != nil && d.config.HasBackupLock() {
		if _, err := d.conn.Exec("LOCK TABLES FOR BACKUP"); err != nil {
			log.Fatalf("Error locking tables for backup: %s", err)
		}
		if _, err := d.conn.Exec("LOCK BINLOG FOR BACKUP"); err != nil {
			log.Fatalf("Error locking binlog for backup: %s", err)
		}
	}
	return d
}

func (d *DirDumper) Dump(tableName interface{}) {
	name := tableName.(string)
	td := table_dumper.NewTableDumper(d.dsn, name, d.config).WithHeader(d.withHeader)
	fileName := name + fileSuffix
	writer, err := d.getWriter(fileName)
	if err != nil {
		log.Fatalf("Error getting writer: %s", err)
	}
	compressor, _ := bzip2.NewWriter(writer, &bzip2.WriterConfig{Level: bzip2.BestCompression})
	dumpResult, err := td.Run(compressor, d.conn, d.withHeader)
	if err != nil {
		log.Printf("Error running worker: %s", err)
		compressor.Close()
		writer.Close()
		return
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
	if command := d.prepareCommand(fileName); command != "" {
		if err := exec.Command("/bin/sh", "-c", command).Start(); err != nil {
			log.Printf("Error starting command %q: %s", command, err)
		} else {
			log.Printf("Started command %q", command)
		}
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

func (d DirDumper) prepareCommand(fileName string) string {
	if d.runAfter == "" {
		return ""
	}
	command := strings.Replace(d.runAfter, "%FILE_NAME%", fileName, -1)
	command = strings.Replace(command, "%FILE_PATH%", d.dir+"/"+fileName, -1)
	return command
}
