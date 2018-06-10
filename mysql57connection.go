// conection implementation for mysql 5.7

package main

import (
	//	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/andsha/mysqlutils"
	"github.com/andsha/replicagor/mysqlconnection"
	"github.com/andsha/replicagor/structs"
	"github.com/andsha/securestorage"
	"github.com/andsha/vconfig"
)

//extends conn structure
type mysqlConnection struct {
	*conn
	blprocess      *mysqlconnection.MysqlProcess
	sqlprocess     *mysqlutils.MysqlProcess
	updateRinfo    chan structs.ST
	sendNewTabInfo chan *structs.Table
	//eventLog *mysqlconnection.EventLog
}

func NewMysqlConnection(c *conn) (*mysqlConnection, error) {
	// gen new mysql connection which expands connection
	mysqlc := new(mysqlConnection)
	mysqlc.conn = c
	updateRinfo := make(chan structs.ST, 1)
	mysqlc.updateRinfo = updateRinfo
	sendNewTabInfo := make(chan *structs.Table, 1)
	mysqlc.sendNewTabInfo = sendNewTabInfo

	// generate query process
	if err := mysqlc.sqlconnect(); err != nil {
		return nil, err
	}

	// fill rinfo
	if err := mysqlc.initInfo(); err != nil {
		return nil, err
	}

	// generate binlog process
	if err := mysqlc.blconnect(); err != nil {
		return nil, err
	}

	return mysqlc, nil
}

func (c *mysqlConnection) blconnect() error {
	// generate binlog process using rinfo
	c.blprocess = mysqlconnection.NewProcess(c.rinfo, c.updateRinfo, c.sendNewTabInfo, c.logging)

	credentials, err := c.getConnCredentials()
	if err != nil {
		return err
	}
	portint, err := strconv.Atoi(credentials["port"])
	if err != nil {
		return errors.New(fmt.Sprintf("Error when converting pot into int: %v", credentials["port"]))
	}
	if err := c.blprocess.ConnectAndAuth(
		credentials["host"],
		portint,
		credentials["user"],
		credentials["password"],
	); err != nil {
		return err
	}
	return nil
}

func (c *mysqlConnection) sqlconnect() error {
	credentials, err := c.getConnCredentials()
	if err != nil {
		return err
	}

	conn, err := mysqlutils.NewDB(
		credentials["host"],
		credentials["port"],
		credentials["user"],
		"",
		credentials["password"],
		nil,
	)
	if err != nil {
		return err
	}
	c.sqlprocess = conn

	return nil

}

func (c *mysqlConnection) getConnCredentials() (map[string]string, error) {
	credentials := make(map[string]string)
	cfg, sec, _, err := c.getHostInfo()
	if err != nil {
		return nil, err
	}

	// get params for mysql connection
	if host, err := sec.GetSingleValue("host", ""); err != nil {
		return nil, err
	} else {
		credentials["host"] = host
	}

	if port, err := sec.GetSingleValue("port", ""); err != nil {
		return nil, err
	} else {
		credentials["port"] = port
	}

	if user, err := sec.GetSingleValue("user", ""); err != nil {
		return nil, err
	} else {
		credentials["user"] = user
	}

	password, err := sec.GetSingleValue("password", "")
	if err != nil {
		return nil, err
	}

	pwdSections, err := cfg.GetSections("SECURE PASSWORD")
	var pwdSection *vconfig.Section
	if err == nil {
		pwdSection = pwdSections[0]
	}

	var keyStorage *securestorage.SecureStorage
	if pwdSection != nil {
		var err error
		keyStorage, err = securestorage.NewSecureStorage("", "", pwdSection)
		if err != nil {
			return nil, err
		}
	}

	if password != "" {
		var err error
		if strings.HasSuffix(password, ".key") {
			password, err = keyStorage.GetPasswordFromFile(password)
		} else {
			password, err = keyStorage.GetPasswordFromString(password)
		}
		if err != nil {
			return nil, err
		}
	}

	credentials["password"] = password

	return credentials, nil
}

func (c *mysqlConnection) disconnect() error {

	return nil
}

func (c *mysqlConnection) startDump(
	stop_d <-chan bool,
	stopped_d chan<- bool,
	stop_uri <-chan bool,
	stopped_uri chan<- bool) (<-chan *structs.Event, error) {
	pos, filename, err := c.blprocess.GetMasterStatus()

	// for 160k events:
	//pos = 1585845
	//filename = "mysql-bin.000006"

	if err != nil {
		return nil, err
	}
	serverId := uint32(2)

	eventlog, err := c.blprocess.StartBinlogDump(pos, filename, serverId)
	if err != nil {
		return nil, err
	}
	echan := eventlog.GetEventChan()
	go eventlog.Start(stop_d, stopped_d)
	go c.UpdateRinfo(stop_uri, stopped_uri)

	return echan, nil
}

func (c *mysqlConnection) getFreqs() []int {
	return c.freqs
}

func (c *mysqlConnection) playEvent(e *structs.Event) error {
	return nil
}

// get structure of source db
func (c *mysqlConnection) getDBInfo(schemas []string) ([]structs.Schema, error) {

	/* fake inmplementation
	ss := make([]structs.Schema, 0)

	slist := []string{"db1", "db4", "db5", "db8", "db9"}

	for _, sname := range slist {
		s := new(structs.Schema)
		s.Name = sname
		ts := make([]*structs.Table, 0)
		for i := 1; i <= 10; i++ {
			t := new(structs.Table)
			t.Name = fmt.Sprintf("tab%v", i)
			fs := make([]*structs.Column, 0)
			for j := 1; j <= 10; j++ {
				f := new(structs.Column)
				f.Name = fmt.Sprintf("f%v", j)
				fs = append(fs, f)
			}
			t.Columns = fs
			ts = append(ts, t)
		}
		s.Tables = ts
		ss = append(ss, *s)
	}
	*/

	// true implementation
	sstructs := make([]structs.Schema, 0)
	for _, schema := range schemas {
		sql := fmt.Sprintf("USE %v", schema)
		if _, err := c.sqlprocess.Run(sql); err == nil {
			sstruct := new(structs.Schema)
			sstruct.Name = schema
			tstructs := make([]*structs.Table, 0)
			sql := "SHOW TABLES"
			res, err := c.sqlprocess.Run(sql)
			if err != nil {
				return nil, err
			}
			//fmt.Println(res)
			for _, tab := range res {
				t := new(structs.Table)
				stab, _ := tab[0].(string)
				t.Name = stab
				cstructs := make([]*structs.Column, 0)
				sql := fmt.Sprintf("SHOW COLUMNS FROM %v", stab)
				res, err := c.sqlprocess.Run(sql)
				if err != nil {
					return nil, err
				}
				for _, col := range res {
					c := new(structs.Column)
					scol, _ := col[0].(string)
					c.Name = scol
					cstructs = append(cstructs, c)
				}
				t.Columns = cstructs
				tstructs = append(tstructs, t)
			}
			sstruct.Tables = tstructs
			sstructs = append(sstructs, *sstruct)
		}

	}

	return sstructs, nil
}

// fill rinfo struct
func (c *mysqlConnection) initInfo() error {
	schemaSections, err := c.rconf.GetSectionsByName("replicatedDatabases")
	if err != nil {
		return err
	}

	schemas, err := schemaSections[0].GetValues("databases")
	if err != nil {
		return err
	}

	sinfo, err := c.getDBInfo(schemas)
	if err != nil {
		return err
	}

	//	for _, s := range sinfo {
	//		for _, t := range s.Tables {
	//			for _, c := range t.Columns {
	//				fmt.Println(s.Name + "." + t.Name + "." + c.Name)
	//			}
	//		}
	//	}

	rinfo := make([]structs.Schema, len(sinfo))
	copy(rinfo, sinfo)

	m, err := getCFGInfo(c.rconf, "excludedTables", "tables")
	if err != nil {
		return err
	}
	excludedTablesMap, _ := m.(map[string]map[string]interface{})

	m, err = getCFGInfo(c.rconf, "enableDelete", "tables")
	if err != nil {
		return err
	}
	enableDeleteTablesMap, _ := m.(map[string]map[string]interface{})

	m, err = getCFGInfo(c.rconf, "excludedColumns", "columns")
	if err != nil {
		return err
	}
	excludedColumnsMap, _ := m.(map[string]map[string]map[string]interface{})

	for idr := range rinfo {
		schema := rinfo[idr]
		for idt, t := range schema.Tables {
			if _, ok := excludedTablesMap[schema.Name][t.Name]; ok {
				rinfo[idr].Tables[idt].ExcludedFromReplication = true
			}
			if _, ok := enableDeleteTablesMap[schema.Name][t.Name]; ok {
				rinfo[idr].Tables[idt].EnableDelete = true
			}
			for idc, c := range t.Columns {
				if _, ok := excludedColumnsMap[schema.Name][t.Name][c.Name]; ok {
					rinfo[idr].Tables[idt].Columns[idc].ExcludedFromReplication = true
				}
			}
		}
	}

	bufferSections, err := c.rconf.GetSectionsByName("buffer")
	if err != nil {
		return err
	}

	bufs := make([][]int, 0)

	for _, bufsec := range bufferSections {
		num, err := bufsec.GetSingleValue("number", "")
		if err != nil {
			return errors.New(fmt.Sprintf("%v. number is a required field in buffer section", err))
		}
		numint, err := strconv.Atoi(num)
		if err != nil {
			return errors.New(fmt.Sprintf("Error while converting number to int. %v", err))
		}
		freq, err := bufsec.GetSingleValue("frequency", "")
		if err != nil {
			return errors.New(fmt.Sprintf("%v. frequency is a required field in buffer section", err))
		}
		freqint, err := strconv.Atoi(freq)
		if err != nil {
			return errors.New(fmt.Sprintf("Error while converting frequency to int. %v", err))
		}
		b := []int{numint, freqint}
		bufs = sintappend(bufs, b, 0)

		schemas, _ := bufsec.GetValues("schemas")
		tables, _ := bufsec.GetValues("tables")

		if len(schemas) != 0 {
			for _, schema := range schemas {
				for idr, r := range rinfo {
					if schema == r.Name {
						rinfo[idr].Buf = numint
						rinfo[idr].Freq = freqint
					}
				}
			}
		}

		if len(tables) != 0 {
			for _, stable := range tables {
				tvar := strings.Split(stable, ".")
				schema := tvar[0]
				table := tvar[1]
				for ids, s := range rinfo {
					if schema == s.Name {
						for idt, t := range s.Tables {
							if table == t.Name {
								rinfo[ids].Tables[idt].Buf = numint
								rinfo[ids].Tables[idt].Freq = freqint
							}
						}
					}
				}
			}

		}

		if len(schemas) == 0 && len(tables) == 0 {
			for ids := range rinfo {
				if rinfo[ids].Buf == 0 {
					rinfo[ids].Freq = freqint
				}
			}
		}
	}

	if bufs[len(bufs)-1][0] > len(bufs)-1 {
		return errors.New(fmt.Sprintf("Buffer numbers must not have missing numbers and shall start from 0. [buf:freq]: %v", bufs))
	}

	if bufs[0][1] != 1 {
		return errors.New(fmt.Sprintf("Frequency of default (0'th) buffer shall be 1"))
	}

	c.rinfo = rinfo

	//	for _, s := range rinfo {
	//		fmt.Println(s.Name)
	//		for _, t := range s.Tables {
	//			fmt.Println(t.Name)
	//			for _, c := range t.Columns {
	//				fmt.Println(c.Name)
	//			}
	//		}
	//	}

	freqs := func(s [][]int) []int {
		a := make([]int, 0)
		for _, v := range s {
			a = append(a, v[1])
		}
		return a
	}(bufs)

	c.freqs = freqs

	return nil
}

func (c *mysqlConnection) GetTableFromRinfo(schema string, table string) *structs.Table {
	for ids := range c.rinfo {
		if c.rinfo[ids].Name == schema {
			for idt := range c.rinfo[ids].Tables {
				if c.rinfo[ids].Tables[idt].Name == table {
					return c.rinfo[ids].Tables[idt]
				}
			}
		}
	}
	return nil
}

// this goroutine listens updateRinfo channel. When receiving event it
// calls GetTableFromRinfo function and sends resulting table to
// sendNewTableInfo channel
func (c *mysqlConnection) UpdateRinfo(stop <-chan bool, stopped chan<- bool) {
	// add stopping on  signal
	for {
		select {
		case <-stop:
			stopped <- true
			return
		case s := <-c.updateRinfo:
			// we can access methods and fields of c from this goroutine witjout mutexes
			// since main thread is waiting for this update and cannot access same fieldsand methods
			if err := c.initInfo(); err != nil {
				c.sendNewTabInfo <- nil
			}
			t := c.GetTableFromRinfo(s.Schema, s.Table)
			c.blprocess.SetRinfo(c.rinfo)
			c.sendNewTabInfo <- t
		default:
			time.Sleep(time.Second * 1)
		}
	}
}

func getCFGInfo(config vconfig.VConfig, secname string, varname string) (interface{}, error) {
	sections, err := config.GetSectionsByName(secname)
	if err != nil {
		return nil, err
	}

	var i interface{}

	switch secname {
	case "excludedTables", "enableDelete":
		mp := make(map[string]map[string]interface{})
		for _, es := range sections {
			tm, err := getCFGSectionInfo(es, "tables")
			if err != nil {
				return nil, err
			}
			m, ok := tm.(map[string]map[string]interface{})
			if !ok {
				return nil, errors.New("excludedTables and enableDelete should have variable 'tables' in following format: schemanane1.tablename1, schemaname1.tablename2, ...")
			}
			for key, val := range m {
				mp[key] = val
			}
		}
		i = mp

	case "excludedColumns":
		mp := make(map[string]map[string]map[string]interface{})
		for _, es := range sections {
			tm, err := getCFGSectionInfo(es, "columns")
			if err != nil {
				return nil, err
			}
			m, ok := tm.(map[string]map[string]map[string]interface{})
			if !ok {
				return nil, errors.New("excludedColumns should have variable 'columns' in following format: schemanane1.tablename1.column1, schemaname1.tablename2.column2, ...")
			}
			for key, val := range m {
				mp[key] = val
			}
		}
		i = mp
	}

	return i, nil
}

func getCFGSectionInfo(sec *vconfig.Section, varname string) (interface{}, error) {

	vals, err := sec.GetValues(varname)
	if err != nil {
		return nil, err
	}

	v := strings.Split(vals[0], ".")
	var m interface{}
	switch len(v) {
	case 1:
		m = make(map[string]interface{})
	case 2:
		tm := make(map[string]map[string]interface{})
		for _, val := range vals {
			v := strings.Split(val, ".")
			schema := v[0]
			table := v[1]
			if _, ok := tm[schema]; ok {
				tm[schema][table] = nil
			} else {
				tm[schema] = map[string]interface{}{}
			}
		}
		m = tm
	case 3:
		tm := make(map[string]map[string]map[string]interface{})
		for _, val := range vals {
			v := strings.Split(val, ".")
			schema := v[0]
			table := v[1]
			column := v[2]

			if _, ok := tm[schema]; ok {
				if _, ok := tm[schema][table]; ok {
					tm[schema][table][column] = nil
				} else {
					tm[schema][table] = map[string]interface{}{}
				}
			} else {
				tm[schema] = map[string]map[string]interface{}{}
			}
			m = tm
		}
	default:
		return nil, errors.New(fmt.Sprintf("unknown structue of %v values in section %v", varname, sec))
	}
	return m, nil

}

func sintappend(sslice [][]int, aslice []int, pos int) [][]int {
	for ids, s := range sslice {
		if aslice[pos] < s[pos] {
			sslice = append(sslice, aslice)
			copy(sslice[ids+1:], sslice[ids:])
			sslice[ids] = aslice
			return sslice
		}
	}
	sslice = append(sslice, aslice)
	return sslice
}
