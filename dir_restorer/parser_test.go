package dir_restorer

import (
	"strings"
	"testing"
	"io/ioutil"
)

func TestParser(t *testing.T) {
	in := []byte(
		"--\n" +
			"-- Table structure for table `chart_mogul_import`\n" +
			"--\n" +
			"\n" +
			"DROP TABLE IF EXISTS `chart_mogul_import`;\n" +
			"/*!40101 SET @saved_cs_client     = @@character_set_client */;\n" +
			"/*!40101 SET character_set_client = utf8 */;\n" +
			"CREATE TABLE `chart_mogul_import` (\n" +
			"  `import_id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
			"  `date_start` datetime DEFAULT NULL,\n" +
			"  `date_end` datetime DEFAULT NULL,\n" +
			"  `status` tinyint(3) DEFAULT NULL,\n" +
			"  `error` longtext,\n" +
			"  PRIMARY KEY (`import_id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=latin1;\n" +
			"/*!40101 SET character_set_client = @saved_cs_client */;\n" +
			"\n" +
			"\n" +
			"--\n" +
			"-- Table structure for table `cb_tasks`\n" +
			"--\n" +
			"\n" +
			"DROP TABLE IF EXISTS `cb_tasks`;\n" +
			"/*!40101 SET @saved_cs_client     = @@character_set_client */;\n" +
			"/*!40101 SET character_set_client = utf8 */;\n" +
			"CREATE TABLE `cb_tasks` (\n" +
			"  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,\n" +
			"  `date` date NOT NULL,\n" +
			"  `date_done` date DEFAULT NULL,\n" +
			"  `name` varchar(60) NOT NULL,\n" +
			"  `campaign_id` int(10) unsigned NOT NULL,\n" +
			"  `worker` varchar(60) NOT NULL DEFAULT '',\n" +
			"  `done` enum('N','Y') NOT NULL DEFAULT 'N',\n" +
			"  `type` tinyint(3) unsigned NOT NULL,\n" +
			"  PRIMARY KEY (`id`),\n" +
			"  UNIQUE KEY `idx_type_campaign_id` (`type`,`campaign_id`),\n" +
			"  KEY `done` (`done`),\n" +
			"  KEY `campaign_id` (`campaign_id`)\n" +
			") ENGINE=InnoDB AUTO_INCREMENT=1298566 DEFAULT CHARSET=utf8;\n" +
			"/*!40101 SET character_set_client = @saved_cs_client */;\n" +
			"	",
	)
	t.Logf("%s", strings.Join(FindTableColumns(in, "cb_tasks"), ", "))
	t.Logf("%s", strings.Join(FindTableColumns(in, "chart_mogul_import"), ", "))
}

func TestFindTables(t *testing.T) {
	schema, err := ioutil.ReadFile("/home/wolf/schema.sql")
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	t.Logf("%s", strings.Join(FindTables(schema), ", "))
}

func TestColumns(t *testing.T) {
	schema, err := ioutil.ReadFile("/home/wolf/schema.sql")
	if err != nil {
		t.Errorf("error reading file: %s", err)
	}
	for _, table := range FindTables(schema) {
		columns := FindTableColumns(schema, table)
		t.Logf("%s: %s", table, strings.Join(columns, ","))
	}
}
