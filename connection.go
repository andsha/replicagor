// factory for connection objects
package main

import (
	"errors"
	"fmt"

	"github.com/andsha/postgresutils"
	"github.com/andsha/vconfig"
)

//connection interface
type connection interface {
	connect() error
	disconnect() error
}

//generic connection data structure
type conn struct {
	// parent of each connection is kept here
	rep *replicagor
}

// Create conection object
func NewConnection(sec *vconfig.Section, r *replicagor) (connection, error) {
	hostName, err := sec.GetSingleValue("host", "")
	if err != nil {
		return nil, err
	}
	hostSections, err := r.config.GetSectionsByVar("host", "name", hostName)
	if err != nil {
		return nil, err
	}
	hostType, err := hostSections[0].GetSingleValue("type", "")
	if err != nil {
		return nil, err
	}

	switch hostType {
	case "postgres":
		c := new(postgres)
		return c, nil
	case "mysql":
		c := new(mysql)
		return c, nil
	default:
		return nil, errors.New(fmt.Sprintf("Cannot resolve connection type: %v", hostType))
	}

}

// ************ connection implementation for postgres *******************
// subclass of connection
type postgres struct {
	conn
	pgprocess *postgresutils.PostgresProcess
}

func (*postgres) connect() error {
	fmt.Println("connect to PSQL")
	return nil
}

func (*postgres) disconnect() error {
	fmt.Println("disconnect from PSQL")
	return nil
}

// ************ connection implementation for mysql *******************
//subclass of connection
type mysql struct {
	conn
	mysqlprocess int
}

func (*mysql) connect() error {
	fmt.Println("connect to MySQL")
	return nil
}

func (*mysql) disconnect() error {
	fmt.Println("disconnect from MySQL")
	return nil
}
