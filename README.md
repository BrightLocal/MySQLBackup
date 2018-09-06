# MySQLBackup

Dumps database tables into compressed data files. Only real tables will be dumped (not views).

Usage:
```
 tabledumper
 -login-path=backup                    # read connection credentials from ~/.mylogin.cnf or ~/.my.cnf
 -hostname=localhost                   # can be unix socket path
 -port=3306                            # ignored if unix socket is used
 -streams=8                            # how many tables to dump in parallel, defaults to the number of CPU cores
 -database=test                        # database to dump
 -tables=table_1,table_2               # list of tables to dump, will do all if skipped
 -skip-tables=temp_table,temp2_table   # will not dump these tables, should be used either -skip-tables or -tables option or none 
 -dir=/path/to/directory               # where to store dumps. sftp://user@host/path/to/directory also supported
 -username=user                        # will be used if no login-path is given
 -password=secret
 -run-after=~/my-script.sh %FILE_PATH% # command to run after a table is dumped, %FILE_NAME% and %FILE_PATH% placeholders available
 -with-header                          # add header with column names to the backup
```
A file will be created for each table using `table_name.csjson.bz2` naming schema.

Each row will be a set of comma separated JSON encoded values:
```
"123","multi line\nvalue",null,""
```
Optionally, with header, when `-with-header` option is used:
```
`col1`,`col2`,`col3`,`col4`
"123","multi line\nvalue",null,""
```
Note: Dumper will try to use Percona's backup locks for consistency of the snapshots.
