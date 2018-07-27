package pgfuncs

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var sysWords []string

func init() {
	sysWords = []string{"select", "from", "where", "group", "sort", "left", "join", "by", "rigth", ",", ")", "(", "sum", "if", "as", "and", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", " ", "\n", "<", "=", ">", "in", "count", ";", "when", "then", "else", "end", "distinct", "-", "extract", "date", "*", "on", ".", "date_trunc", "table", "create", "primary", "key", "index",
		"decimal", "varchar", "integer", "bigint", "timestamp", "int", "not", "schema", "set", "default", "null", "to", "insert", "into", "current_timestamp", "values", "now", "alter", "add",
		"unique", "type", "text", "drop", "exists", "delete", "column", "update", "is", "ifnull", "coalesce", "least", "greatest", "or", "like", "including", "all", "first", "constraint", "rename", "procedure",
		"localtimestamp", "smallint", "char", "/*", "*/", "temporary", "desc", "using", ":", "tablespace", "modify", "view", "/", "replace", "case", "lower", "trim", "regexp_replace", "round", "||", "begin", "commit"}
}

func getReplaces() [][]string {
	var replaces [][]string

	r := []string{`0000-00-00 00:00:00`, `1957-01-01 00:00:00`} //replace 0000 year with 1957-01-01
	replaces = append(replaces, r)
	//r = []string{`DROP`, `DASHA`} //replace 0000 year with 1957-01-01
	//replaces = append(replaces, r)

	r = []string{`tinyint\(\d*\)`, `int`} // replace tinyint with int
	replaces = append(replaces, r)
	r = []string{`smallint\(\d*\)`, `int`} // replace tinyint with int
	replaces = append(replaces, r)
	r = []string{`longtext`, `text`} // replace longtext with text
	replaces = append(replaces, r)
	r = []string{`longblob([, ])`, `text$1`} // replace longtext with text
	replaces = append(replaces, r)
	r = []string{`blob([, ])`, `text$1`} // replace longtext with text
	replaces = append(replaces, r)
	r = []string{`ALGORITHM.*SQL SECURITY DEFINER`, ``} // remove definer for the view
	replaces = append(replaces, r)
	r = []string{`COMMENT '[a-zA-Z\s0-9\/]*'`, ``} // remove comment
	replaces = append(replaces, r)
	r = []string{`CREATE\s*VIEW`, `CREATE OR REPLACE VIEW`} // replace CREATE VIEW with CREATE OR REPLACE VIEW
	replaces = append(replaces, r)
	r = []string{`/\*!50001 ([a-zA-Z0-9\s_]*)\*/`, `$1`} // add support for /*!50001 DROP VIEW IF EXISTS cisOffers*/
	replaces = append(replaces, r)
	r = []string{`lcase`, `lower`} // lcase to lower
	replaces = append(replaces, r)
	r = []string{`ifnull\(([^,\)\(]*),([^,\)\(]*)\)`, `case when $1 is null then $2 else $1 end`} // replace ifnull and if with case statements
	replaces = append(replaces, r)
	r = []string{`ifnull\(([^,\)\(]*),([^,\)\(]*)\)`, `case when $1 is null then $2 else $1 end`} // replace ifnull and if with case statements
	replaces = append(replaces, r)
	r = []string{`nullif\(([^,\)\(]*),([^,\)\(]*)\)`, `case when $1 is null then $2 else $1 end`} // replace ifnull and if with case statements
	replaces = append(replaces, r)
	r = []string{`if\(([^,]*),([^,\)\(]*),([^,\)\(]*)\)`, `case when $1 is null then $2 else $3 end`} // replace ifnull and if with case statements
	replaces = append(replaces, r)
	r = []string{`if\(([^,]*),([^,\)\(]*),([^,\)\(]*)\)`, `case when $1 is null then $2 else $3 end`} // replace ifnull and if with case statements
	replaces = append(replaces, r)
	r = []string{`case when ([^\s=]*) then (.*) end`, `case when $1=1 then $2 end`} // replace case where no condition
	replaces = append(replaces, r)
	r = []string{`case when ([^\s=]*) then (.*) end`, `case when $1=1 then $2 end`} // case when cis.studios.is_licensor then `true` else `false` end
	replaces = append(replaces, r)
	r = []string{`straight_join`, `join`} // straight_join to join
	replaces = append(replaces, r)
	r = []string{`trailing\s([^\s]*)\sfrom\s([^\s\)]*)`, `regexp_replace($2,$1,'')`} //replace trailing with regexp_replace
	replaces = append(replaces, r)
	r = []string{`format\(\(`, `ROUND((0.01*`} //format to round
	replaces = append(replaces, r)
	r = []string{`concat\(([^,]*),([^,]*),([^,]*),([^,]*),([^,]*)\),`, `($1||$2||($3)||$4||($5)),`} //replcase concat
	replaces = append(replaces, r)
	r = []string{`if\(([^,]*),([^,]*),([^,]*)\) AS`, `case when $1 is null then $2 else $3 end AS `} //replcase concat
	replaces = append(replaces, r)
	r = []string{"`", `"`} //quotes from Mysqlreplace to double quotes
	replaces = append(replaces, r)
	r = []string{`"_([a-zA-Z_]+)_new`, `"$1`} //rename table for percona upgrades to normal table
	replaces = append(replaces, r)
	r = []string{`\._([a-zA-Z_]+)_new`, `.$1`} // rename table for percona upgrades to normal table
	replaces = append(replaces, r)
	r = []string{`'_([a-zA-Z_]+)_new`, `'$1`} // rename table for percona upgrades to normal table
	replaces = append(replaces, r)
	r = []string{`CHARACTER SET [a-z0-9]+`, ``} // remove charachter sets
	replaces = append(replaces, r)
	r = []string{`DEFAULT CHARSET=[a-z0-9]+`, ``} // remove charachter sets
	replaces = append(replaces, r)
	r = []string{`(MODIFY[^,]*) DEFAULT [a-zA-Z0-9'\_]+`, `$1`} // we dont need default value for alter, we replicate complete record from mysql
	replaces = append(replaces, r)
	r = []string{`(CHANGE[^,]*) DEFAULT [a-zA-Z0-9'\_]+`, `$1`} // we dont need default value for alter, we replicate complete record from mysql
	replaces = append(replaces, r)
	r = []string{`bigint\(\d*\)`, `bigint`} // remove size for int
	replaces = append(replaces, r)
	r = []string{`int\(\d*\)`, `bigint`} // remove size for int
	replaces = append(replaces, r)
	r = []string{`UNSIGNED`, ``} //remove unsigned
	replaces = append(replaces, r)
	r = []string{` DATETIME`, ` timestamp`} //datetime to timestamp
	replaces = append(replaces, r)
	r = []string{`ENGINE=[a-z]+`, ``} //emove engine
	replaces = append(replaces, r)
	r = []string{`,\s*\)`, `)`} // if we have `,)` than we dont need comma
	replaces = append(replaces, r)
	r = []string{`enum\s*\([^)]*\)`, `varchar(100)`} //replace all enums to varchar 100
	replaces = append(replaces, r)
	r = []string{`""`, `''''`} //replace empty string in MySQL to empty string in postgres
	replaces = append(replaces, r)
	r = []string{`MODIFY COLUMN`, `MODIFY`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+) NOT NULL DEFAULT ([a-zA-Z0-9"']+)`, `ALTER COLUMN $1 TYPE $2, ALTER COLUMN $1 SET NOT NULL,ALTER COLUMN $1 SET DEFAULT $3`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+) DEFAULT ([a-zA-Z0-9"']+)`, `ALTER COLUMN $1 TYPE $2,ALTER COLUMN $1 SET DEFAULT $3`} //modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+) NOT NULL`, `ALTER COLUMN $1 TYPE $2, ALTER COLUMN $1 SET NOT NULL`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+)\s+NULL`, `ALTER COLUMN $1 TYPE $2, ALTER COLUMN $1 DROP NOT NULL`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+)\s+DEFAULT\s+NULL`, `ALTER COLUMN $1 TYPE $2, ALTER COLUMN $1 SET DEFAULT NULL`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+)`, `ALTER COLUMN $1 TYPE $2`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+),`, `ALTER COLUMN $1 TYPE $2,`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`MODIFY\s+([a-z"_]+)\s+([a-zA-Z0-9(),]+);`, `ALTER COLUMN $1 TYPE $2;`} // modify all MODIFY statements
	replaces = append(replaces, r)
	r = []string{`CHANGE ([a-zA-Z"]+) ([a-zA-Z]+) VARCHAR\((\d+)\)`, `DROP COLUMN $1, ADD COLUMN $2 VARCHAR($3)`} //modify change column to rename
	replaces = append(replaces, r)
	r = []string{`CHANGE\s+COLUMN\s+([a-zA-Z]+)\s+([a-zA-Z]+)\s+([a-zA-Z0-9()]+)\s*(unsigned)*(NOT)*\s*(NULL)*`, `RENAME COLUMN $1 TO $2`} //
	replaces = append(replaces, r)
	r = []string{`ALTER TABLE \S+\s{0,};`, ``} //remove alter table statement if it is empty (we had only indexes there)
	replaces = append(replaces, r)
	r = []string{`database `, `schema `} //replace database to schema
	replaces = append(replaces, r)
	r = []string{`mediumtext`, `text`} // medium text to text
	replaces = append(replaces, r)
	r = []string{`,;`, `;`} // remove unnecessary comma at the end
	replaces = append(replaces, r)
	r = []string{`\/\*[^\/^\*]+\*\/`, ``} //remove comments /* */
	replaces = append(replaces, r)
	r = []string{`float`, `decimal`} // change float to decimal
	replaces = append(replaces, r)
	r = []string{`double`, `decimal`} // double to decimal
	replaces = append(replaces, r)
	r = []string{`rename table ([a-z]+) to ([a-z]+)`, `ALTER TABLE $1 RENAME TO $2`} //handle rename table
	replaces = append(replaces, r)
	r = []string{`AUTO_INCREMENT(=\d+)?`, ``} //remove AUTO INCREMENT
	replaces = append(replaces, r)
	r = []string{`ALTER COLUMN ([a-z]+) TYPE int`, `ALTER COLUMN $1 TYPE int USING ($1::integer)`} //
	replaces = append(replaces, r)
	r = []string{`ON UPDATE CURRENT_TIMESTAMP`, ``} //we don`t need statemnet how to update timestamp
	replaces = append(replaces, r)
	r = []string{` change ([a-z0-9_]+) ([a-z0-9_]+) ([a-z0-9_]+)`, ` rename column $1 TO $2`} //
	replaces = append(replaces, r)

	return replaces
}

func ConvertMysql57ToPostgres(mysqlscript string, tablespace bool) (string, error) {
	updateId := `0`
	//updateId = strings.Split(strings.Split(mysqlscript, `_`)[1], `.`)[0]

	mysqlscript = regexp.MustCompile(`(?i)alter table\s{1,}([^\s/(]*)`).ReplaceAllString(mysqlscript, ` \n alter table $1 \n`)
	mysqlscript = regexp.MustCompile(`(?i)create table\s{1,}([^\s/(]*)`).ReplaceAllString(mysqlscript, ` \n create table $1 \n`)
	mysqlscript = regexp.MustCompile(`(?i)[^drop ]primary key`).ReplaceAllString(mysqlscript, ` \n primary key`)
	mysqlscript = regexp.MustCompile(`(?i)(add)?\s+unique (key|index)`).ReplaceAllString(mysqlscript, ` \n unique key`)
	mysqlscript = regexp.MustCompile(`(?i)[^primary|unique|add] key `).ReplaceAllString(mysqlscript, ` \n key `)
	mysqlscript = regexp.MustCompile(`(?i)[^ADD|DROP|CREATE] index `).ReplaceAllString(mysqlscript, ` \n index `)
	if !strings.HasPrefix(strings.ToLower(mysqlscript), `add`) {
		mysqlscript = regexp.MustCompile(`\)\s{0,5}\)$`).ReplaceAllString(mysqlscript, ` )\n)\n`)
	}
	mysqlscripts := strings.Split(mysqlscript, `\n`)

	indexre := regexp.MustCompile(`.*(\s|^)(index|key)[\s\(]+.*`)    // check for lower case
	pkeyre := regexp.MustCompile(`.*(\s|^)primary\s+key([\s\(]+).*`) // check for lower case

	var scripts string
	var database string
	var table string
	var indexes string
	var pkey string

	for _, mysqls := range mysqlscripts {
		mysqls = strings.Trim(mysqls, ` `)
		if len(mysqls) == 0 {
			continue
		}
		lmysqls := strings.ToLower(mysqls)

		if strings.Contains(lmysqls, `convert to character set`) {
			continue
		}

		if strings.Contains(lmysqls, `function`) {
			scripts = ``
			break
		}

		if strings.Contains(lmysqls, `set search_path`) {
			database = strings.Replace(strings.Split(strings.Trim(lmysqls, ` `), ` `)[3], `;`, ``, -1)
		}

		if strings.Contains(lmysqls, `alter table`) || strings.Contains(lmysqls, `create table`) {
			table = strings.Split(strings.Trim(lmysqls, ` `), ` `)[2]
			table = strings.Split(table, `(`)[0]
			if strings.Contains(table, `.`) {
				ts := strings.Split(table, `.`)
				database = ts[0]
				table = ts[1]
			}
			table = strings.Replace(table, "`", ``, -1)
		}

		if !strings.HasPrefix(mysqls, `--`) && !strings.HasPrefix(mysqls, `#`) {
			if indexre.MatchString(lmysqls) && !pkeyre.MatchString(lmysqls) {
				if len(indexre.FindAllString(lmysqls, -1)) > 1 {
					return "", errors.New(`Looks like we have several index statements in one line. Please make one line per index`)
				}
				if i, err := getIndex(table, mysqls, database, tablespace); err != nil {
					return "", err
				} else {
					indexes += i
				}
			} else if strings.Contains(lmysqls, `drop primary key`) {
				if i, err := getIndex(table, mysqls, database, tablespace); err != nil {
					return "", err
				} else {
					scripts += i
				}
			} else if strings.Contains(lmysqls, `primary key`) {
				pkey = getPKey(table, mysqls)
				mysqls = strings.Replace(mysqls, pkey, ``, -1)
				scripts += strings.Replace(mysqls, `\n`, ` `, -1) + ` `
			} else {
				scripts += strings.Replace(mysqls, `\n`, ` `, -1) + ` `
			}
		}

	}

	mysqls := strings.Trim(scripts, ` `)
	//fmt.Println("m", mysqls)

	if !strings.HasSuffix(mysqls, `;`) && len(scripts) != 0 {
		mysqls += `;`
	}

	if len(table) == 0 {
		t := regexp.MustCompile(`TABLE\s+IF\s+EXISTS\s+([a-z0-9]+)`).FindStringSubmatch(mysqls)
		if len(t) > 0 {
			table = t[1]
		}
	}
	if len(table) == 0 {
		t := regexp.MustCompile(`TABLE\s+([a-z0-9]+)`).FindStringSubmatch(mysqls)
		if len(t) > 0 {
			table = t[1]
		}
	}

	//mysqls += `\n` + indexes + `\nupdate replication.upgrades set tbl='` + database + `.` + table + `' where ` + `"upgradeId"` + `=` + updateId + `;`
	if len(indexes) != 0 {
		mysqls += `\n` + indexes + `;`
	}
	//fmt.Println("m", mysqls)

	//fmt.Println(mysqls)
	//fmt.Println(pkey)
	if len(pkey) != 0 { // do something about tablespace
		mysqls += fmt.Sprintf(`\nALTER INDEX %v SET TABLESPACE index_tablespace`, pkey)
	}

	//fmt.Println(mysqls)
	//fmt.Println(pkey)

	for _, r := range getReplaces() {
		//fmt.Println("replaces:", r)
		re := regexp.MustCompile(fmt.Sprintf("(?i)%v", r[0]))
		mysqls = re.ReplaceAllString(mysqls, r[1])
		//fmt.Println(r, mysqls)

		re = regexp.MustCompile(fmt.Sprintf("(?i)%v", r[0]))
	}

	var db string
	t := regexp.MustCompile(`SET search_path TO "([a-z0-9]+)";`).FindStringSubmatch(mysqls)
	if len(t) > 0 {
		db = t[1]
	}

	t = regexp.MustCompile(`RENAME COLUMN ([a-zA-Z0-9]+) TO ([a-z0-9]+)`).FindStringSubmatch(mysqls)
	if len(t) > 0 {
		oldcol := t[1]
		newcol := t[2]
		mysqls += fmt.Sprintf(`INSERT INTO replication.alter_table("upgradeId" ,db,table_name,column_name,action,new_name)
            VALUES ('%v','%v','%v','%v','RENAME','%v'));`, updateId, db, table, oldcol, newcol)
	}

	t = regexp.MustCompile(`DROP\s+COLUMN\s+([a-zA-Z0-9]+)`).FindStringSubmatch(mysqls)

	if len(t) > 0 {
		col := t[1]
		mysqls += fmt.Sprintf(`INSERT INTO replication.alter_table("upgradeId" ,db,table_name,column_name,action,new_name)
            VALUES ('%v','%v','%v','%v','DROP COLUMN',''));`, updateId, db, table, col)
	}

	var res string
	for _, word := range getWords(mysqls) {
		//fmt.Println("word:", word, len(word))
		_, err := strconv.ParseFloat(word, 64)
		if strSliceHasItem(sysWords, strings.ToLower(word)) ||
			len(strings.Trim(word, " ")) == 0 ||
			word[:1] == "\"" ||
			word[:1] == "'" ||
			word[:1] == "@" ||
			err == nil {
			res += word
		} else {
			res += `"` + word + `"`
		}
	}

	return strings.Replace(res, `\n`, "\n", -1), nil
}

func strSliceHasItem(slice []string, s string) bool {
	for _, str := range slice {
		if str == s {
			return true
		}
	}
	return false
}

func getIndex(table, mysqls, database string, tablespace bool) (string, error) {
	mysqls = strings.Trim(mysqls, " ")
	smysqls := strings.ToLower(mysqls)

	if strings.Contains(smysqls, "foreign key") {
		return ``, nil
	}

	table = strings.Trim(table, " ")

	if strings.Contains(smysqls, "drop index") {
		idxname := table + "_" + strings.Split(strings.Split(mysqls, "DROP")[1], " ")[2]
		if len(idxname) > 63 {
			idxname = idxname[len(idxname)-64:]
		}
		return "DROP INDEX" + idxname + ";", nil
	}

	if strings.Contains(smysqls, "drop primary key") {
		return fmt.Sprintf("DROP CONSTRAINT %v_pkey;", table), nil
	}

	if strings.Contains(smysqls, "create index") {
		if tablespace {
			return mysqls + "TABLESPACE index_tablespace;", nil
		} else {
			return mysqls + ";", nil
		}
	}

	indexre := regexp.MustCompile(`(\s|^)(unique\s)?(index|key)(\s+([^\(\)\s]+))?\s*(on\s+([a-zA-Z_0-9]*))?\((.*)\s*\)`)
	idxmatch := indexre.FindStringSubmatch(mysqls)

	if idxmatch == nil {
		return "", errors.New("index definition not found")
	}

	idxname := idxmatch[5]
	if len(idxmatch) >= 8 {
		table = idxmatch[7]
	}
	fields := idxmatch[8]
	fields = regexp.MustCompile("(?i`)").ReplaceAllString(fields, `"`)
	fields = regexp.MustCompile(`(?i)("?[a-zA-Z0-9_]+"?)\((\d*)\)`).ReplaceAllString(mysqls, `left($1,$2)`)
	if len(idxname) == 0 {
		idxname = table
		for _, field := range strings.Split(fields, ",") {
			idxname += "_" + strings.Trim(field, " ")
		}
	} else {
		idxname = table + "_" + idxname
	}

	if len(idxname) > 63 {
		idxname = idxname[len(idxname)-64:]
	}

	sql := fmt.Sprintf(`CREATE INDEX %v ON %v.%v (%v)`, idxname, database, table, fields)
	if tablespace {
		return sql + " TABLESPACE index_tablespace;", nil
	} else {
		return sql + ";", nil
	}
}

func getPKey(table, mysqls string) string {
	mysqls = strings.Trim(mysqls, " ")
	mysqls = strings.Replace(strings.Replace(mysqls, "(", " $", -1), ")", "$", -1)

	var s string
	for _, m := range strings.Split(mysqls, " ") {
		if m != "" &&
			strings.ToLower(m) != "primary" &&
			strings.ToLower(m) != "key" &&
			strings.ToLower(m) != "add" {
			s = m
		}
	}
	pkeyname := s

	if s[:1] == "$" {
		pkeyname = table + "_pkey"
	}

	return pkeyname
}

func getWords(mysqls string) []string {
	var res []string
	separators := []string{",", " ", "(", ")", "\n", ";", ">", "<", "=", ".", `"`, ":", "*", "||", "'"}
	var word string
	check_quot := 0
	for _, s := range mysqls {
		sym := string(s)
		if strSliceHasItem(separators, sym) {
			if check_quot == 0 || sym == "'" || sym == `"` {
				if (sym == "'" || sym == `"`) && check_quot == 0 {
					check_quot = 1
					res = append(res, word)
					word = sym
					continue
				}

				if (sym == "'" || sym == `"`) && check_quot == 1 {
					check_quot = 0
					word += sym
					continue
				}

				if word != "" {
					res = append(res, word)
					word = ""
				}

				res = append(res, sym)
			} else {
				word += sym
			}
		} else {
			word += sym
		}
	}

	return res

}
