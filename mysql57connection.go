// conection implementation for mysql 5.7

package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/andsha/replicagor/mysqlconnection"
	"github.com/andsha/securestorage"
	"github.com/andsha/vconfig"
)

//extends conn structure
type mysqlConnection struct {
	conn
	config  *vconfig.VConfig
	process *mysqlconnection.MysqlProcess
}

func NewMysqlConnection(t int, cfg *vconfig.VConfig) (*mysqlConnection, error) {

	c := new(mysqlConnection)
	c.config = cfg
	c.connType = t
	process := new(mysqlconnection.MysqlProcess)
	c.process = process
	return c, nil
}

func (c *mysqlConnection) connect() error {
	var t string
	if c.connType == SOURCE {
		t = "source"
	} else {
		t = "destination"
	}

	hostType, err := c.config.GetSingleValue(t, "host", "")
	if err != nil {
		return err
	}

	mysqlHostSections, err := c.config.GetSectionsByVar("host", "type", hostType)
	if err != nil {
		return err
	}

	// get params for mysql connection
	host, err := mysqlHostSections[0].GetSingleValue("host", "")
	if err != nil {
		return err
	}

	port, err := mysqlHostSections[0].GetSingleValue("port", "")
	if err != nil {
		return err
	}
	portint, err := strconv.Atoi(port)
	if err != nil {
		return errors.New(fmt.Sprintf("Cannot convert port %v to int", port))
	}

	user, err := mysqlHostSections[0].GetSingleValue("user", "")
	if err != nil {
		return err
	}

	password, err := mysqlHostSections[0].GetSingleValue("password", "")
	if err != nil {
		return err
	}

	pwdSections, err := c.config.GetSections("SECURE PASSWORD")
	var pwdSection *vconfig.Section
	if err == nil {
		pwdSection = pwdSections[0]
	}

	var keyStorage *securestorage.SecureStorage
	if pwdSection != nil {
		var err error
		keyStorage, err = securestorage.NewSecureStorage("", "", pwdSection)
		if err != nil {
			return err
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
			return err
		}
	}

	// connect to mysql
	if err := c.process.ConnectAndAuth(host, portint, user, password); err != nil {
		return err
	}

	fmt.Println("Mysql:", c)

	return nil
}

func (c *mysqlConnection) disconnect() error {

	return nil
}

func (c *mysqlConnection) getSourceInfo(schemas []string) ([]schema, error) {
	// fake inmplementation
	ss := make([]schema, 0)

	slist := []string{"db1", "db4", "db5", "db8", "db9"}

	for _, sname := range slist {
		var s schema
		s.name = sname
		ts := make([]table, 0)
		for i := 1; i <= 10; i++ {
			var t table
			t.name = fmt.Sprintf("tab%v", i)
			fs := make([]field, 0)
			for j := 0; j <= 10; j++ {
				var f field
				f.name = fmt.Sprintf("f%v", j)
				fs = append(fs, f)
			}
			t.fields = fs
			ts = append(ts, t)
		}
		s.tables = ts
		ss = append(ss, s)
	}

	return ss, nil
}

func (c *mysqlConnection) startDump() (<-chan interface{}, error) {
	pos, filename, err := c.process.GetMasterStatus()
	if err != nil {
		return nil, err
	}
	serverId := uint32(2)
	eventlog, err := c.process.StartBinlogDump(pos, filename, serverId)
	if err != nil {
		return nil, err
	}
	return eventlog.GetEventChan(), nil

}

func (c *mysqlConnection) playEvent(e interface{}) error {
	return nil
}
