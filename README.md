# MySQLBackup

Dumps database tables into compressed data files. Only real tables will be dumped (not views).

Usage:
```
 tabledumper \
 -login-path=backup \                  # read connection credentials from ~/.mylogin.cnf or ~/.my.cnf
 -hostname=localhost \
 -port=3306 \
 -streams=8                            # how many tables to dump in parallel
 -database=test \
 -skip-tables=temp_table,temp2_table \ # will not dump these tables
 -dir=/path/to/directory \             # where to store dumps. sftp://user@host/path/to/directory also supported
 -username=user \                      # will be used if no login-path is given
 -password=secret
```
A file will be created for each table using `table_name.csjson.gz` naming schema.

Each row will be a set of comma separated JSON encoded values:
```
"123","multi line\nvalue",null,""
```

Note: Dumper will try to use Percona's backup locks for consistency of the snapshots.
