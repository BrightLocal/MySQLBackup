package dir_restorer

import (
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"time"

	"github.com/BrightLocal/MySQLBackup/table_restorer"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type DirRestorer struct {
	dsn           string
	dir           string
	schema        []byte
	conn          *sqlx.DB
	totalRows     int
	totalBytes    int
	totalDuration time.Duration
}

func NewDirRestorer(dir string) *DirRestorer {
	r := &DirRestorer{
		dir: strings.TrimRight(dir, "/"),
	}
	var err error
	if r.schema, err = ioutil.ReadFile(dir + "/schema.sql"); err != nil {
		log.Fatalf("error reading schema file: %s", err)
	}
	return r
}

func (d *DirRestorer) Connect(dsn string) *DirRestorer {
	d.dsn = dsn
	//var err error
	//d.conn, err = sqlx.Connect("mysql", d.dsn)
	//if err != nil {
	//	log.Fatalf("Error connecting: %s", err)
	//}
	return d
}

func (d *DirRestorer) getReader(fileName string) (io.ReadCloser, error) {
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
		return d.getSFTPReader(fileName, where)
	}
	return d.getFileReader(fileName)
}

func (d *DirRestorer) getSFTPReader(fileName string, where *url.URL) (io.ReadCloser, error) {
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
	return f.Open(where.Path + "/" + fileName)
}

func (d *DirRestorer) getFileReader(fileName string) (io.ReadCloser, error) {
	return os.Open(fileName)
}

func (d *DirRestorer) Restore(tableName interface{}) {
	name := tableName.(string)
	path, err := filepath.Glob(d.dir + "/" + name + ".*")
	if err != nil {
		log.Printf("error finding file for table %q: %s", name, err)
		return
	}
	if len(path) == 0 {
		log.Printf("file for table %q not found", name)
		return
	}
	if len(path) > 1 {
		log.Printf("found multiple potential files for table %q: %s", name, strings.Join(path, ", "))
		return
	}
	fileName := path[0]
	log.Printf("Detected file %q", fileName)
	var decompressor io.Reader
	reader, err := d.getReader(fileName)
	if err != nil {
		log.Fatalf("Error getting reader: %s", err)
	}
	switch {
	case strings.HasSuffix(fileName, ".bz2"):
		decompressor = bzip2.NewReader(reader)
	case strings.HasSuffix(fileName, ".gz"):
		decompressor, _ = gzip.NewReader(reader)
	}
	if decompressor == nil {
		log.Printf("could not detect decompressing argo for file %q", fileName)
		return
	}
	tr := table_restorer.New(d.dsn, name, FindTableColumns(d.schema, name))
	restoreResult, err := tr.Run(decompressor, d.conn)
	if err != nil {
		log.Printf("Error running worker: %s", err)
		reader.Close()
		return
	}
	d.totalBytes += restoreResult.Bytes()
	d.totalRows += restoreResult.Rows()
	d.totalDuration += restoreResult.Duration()
	if err := reader.Close(); err != nil {
		log.Printf("warning: error closing file reader: %s", err)
	}
}

func (d *DirRestorer) Tables() []string {
	return d.findTables(d.schema)
}

func (DirRestorer) findTables(sql []byte) []string {
	tables := []string{}
	t := rTables.FindAllSubmatch(sql, -1)
	for _, r := range t {
		if len(r) > 1 {
			tables = append(tables, string(r[1]))
		}
	}
	return tables
}

func (d DirRestorer) PrintStats(streams int, totalDuration time.Duration) {
	log.Printf(
		"Restored %d rows (%d bytes) using %d streams in %s (total run time %s)",
		d.totalRows,
		d.totalBytes,
		streams,
		d.totalDuration,
		totalDuration,
	)
}
