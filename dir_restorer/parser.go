package dir_restorer

import (
	"regexp"
	"strings"
)

var (
	rFields = regexp.MustCompile("^\\s+`([^`]+)`")
	rTables = regexp.MustCompile("CREATE TABLE `([^`]+)`")
)

func FindTableColumns(sql []byte, tableName string) []string {
	rParser := regexp.MustCompile("CREATE TABLE `" + tableName + "`[^;]+;")
	m := rParser.FindAllSubmatch(sql, -1)
	fields := []string{}
	if len(m) > 0 && len(m[0]) > 0 {
		for _, line := range strings.Split(string(m[0][0]), "\n") {
			f := rFields.FindAllStringSubmatch(line, -1)
			if len(f) > 0 && len(f[0]) > 1 {
				fields = append(fields, f[0][1])
			}
		}
	}
	return fields
}


