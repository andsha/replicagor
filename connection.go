// factory for connection objects
package main

import (
	//	"bytes"
	"errors"
	"fmt"

	"github.com/andsha/replicagor/structs"
	"github.com/andsha/vconfig"
	"github.com/sirupsen/logrus"
)

const (
	DEST   = 1002
	SOURCE = 1003
)

//connection interface
type connection interface {
	//implemented by both source and destination
	blconnect() error
	sqlconnect() error
	disconnect() error
	getConnCredentials() (map[string]string, error)

	// implemented by source only
	startDump(<-chan bool, chan<- bool, <-chan bool, chan<- bool) (<-chan *structs.Event, error)
	getFreqs() []int

	//implemented by destination only
	playEvent(e *structs.Event) error
}

//generic connection data structure
type conn struct {
	//rep      *replicagor // parent
	logging  *logrus.Logger
	connType int              // type of connection
	rconf    vconfig.VConfig  // replication config
	sconf    vconfig.VConfig  // source config
	rinfo    []structs.Schema // replication info
	freqs    []int
}

// Create conection object
func NewConnection(conntype int, sconf vconfig.VConfig, rconf vconfig.VConfig, logging *logrus.Logger) (connection, error) {
	c := new(conn)
	c.connType = conntype
	c.rconf = rconf
	c.sconf = sconf
	c.logging = logging

	_, _, hostType, err := c.getHostInfo()
	if err != nil {
		return nil, err
	}

	switch hostType {
	case "postgres":
		c, err := NewPgConnection(c)
		if err != nil {
			return nil, err
		}
		return c, nil
	case "mysql":
		// create MySQL connection
		c, err := NewMysqlConnection(c)
		if err != nil {
			return nil, err
		}
		return c, nil

	default:
		return nil, errors.New(fmt.Sprintf("Cannot resolve connection type: %v", hostType))
	}

}

func (c *conn) getHostInfo() (vconfig.VConfig, *vconfig.Section, string, error) {
	var cType string

	if c.connType == SOURCE {
		cType = "source"
	} else if c.connType == DEST {
		cType = "destination"
	} else {
		return nil, nil, "", errors.New("Cannot recognise type of connection. Must be SOURCE or DEST")
	}

	var cfg vconfig.VConfig
	_, err := c.sconf.GetSectionsByName(cType)
	//fmt.Println(c.sconf.ToString())
	//fmt.Println(c.rconf.ToString())

	if err != nil {
		if _, err := c.rconf.GetSectionsByName(cType); err != nil {
			return nil, nil, "", errors.New(fmt.Sprintf("Cannot find section %v in rconf or sconf", cType))
		} else {
			cfg = c.rconf
		}
	} else {
		cfg = c.sconf
	}

	hostType, err := cfg.GetSingleValue(cType, "host", "")
	if err != nil {
		return nil, nil, "", err
	}

	hsec, err := cfg.GetSectionsByVar("host", "type", hostType)
	if err != nil {
		return nil, nil, "", err
	}

	return cfg, hsec[0], hostType, nil

}
