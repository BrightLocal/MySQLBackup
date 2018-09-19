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

## tablerestorer

Usage:
```
  -create
    	Create tables if they do not exist
  -database string
    	Database name to restore
  -dir string
    	Source directory path (default ".")
  -dry-run
    	Dry run with print SQL into stdout
  -filter string
    	Filter rows by expression
  -hostname string
    	Host name (default "localhost")
  -login-path string
    	Login path
  -password string
    	Password
  -port int
    	Port number (default 3306)
  -skip-tables string
    	Table names to skip (incompatible with -tables)
  -streams int
    	How many tables to restore in parallel (default 8)
  -tables string
    	Tables to restore (incompatible with -skip-tables)
  -truncate
    	Clear tables before restoring
  -username string
    	User name
```

### -filter

This option allows sql like expression for filter rows.

Examples:

  * `field == "value"`
  * `NOT field == "value"` or `NOT (field == "value")`
  * `field1 == "value" AND field2 >= 123`
  * `field1 == "value" OR f2 != 435 AND field2 >= 123`
  * `field1 IN ("value", "v2")`
  * `field1 LIKE "%value_x%"`
