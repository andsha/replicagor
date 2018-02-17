// factory for connection objects
package main

import (
	//	"bytes"
	"errors"
	"fmt"

	"github.com/andsha/vconfig"
)

const (
	DEST   = 1002
	SOURCE = 1003
)

//connection interface
type connection interface {
	//implemented by both source and destination
	connect() error
	disconnect() error

	// implemented by source only
	getSourceInfo([]string) ([]schema, error)
	startDump() (<-chan interface{}, error)

	//implemented by destination only
	playEvent(e interface{}) error
}

//generic connection data structure
type conn struct {
	rep      *replicagor // parent
	connType int         // type of connection
}

// Create conection object
func NewConnection(t int, cfg *vconfig.VConfig) (connection, error) {
	var cType string
	if t == SOURCE {
		cType = "source"
	} else if t == DEST {
		cType = "destination"
	} else {
		return nil, errors.New("Cannot recognise type of connection. Must be SOURCE or DEST")
	}

	hostType, err := cfg.GetSingleValue(cType, "host", "")
	if err != nil {
		return nil, err
	}

	switch hostType {
	case "postgres":
		c, err := NewPgConnection(t, cfg)
		if err != nil {
			return nil, err
		}

		return c, nil
	case "mysql":
		// create MySQL connection
		c, err := NewMysqlConnection(t, cfg)
		if err != nil {
			return nil, err
		}

		return c, nil

	default:
		return nil, errors.New(fmt.Sprintf("Cannot resolve connection type: %v", hostType))
	}

}
