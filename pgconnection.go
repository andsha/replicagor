// postgres connection implementation

package main

import (
	"fmt"

	"github.com/andsha/postgresutils"
	"github.com/andsha/vconfig"
)

// extends conn
type pgConnection struct {
	conn
	config  *vconfig.VConfig
	process *postgresutils.PostgresProcess
}

func NewPgConnection(t int, cfg *vconfig.VConfig) (*pgConnection, error) {
	c := new(pgConnection)
	c.config = cfg
	c.connType = t
	c.process = new(postgresutils.PostgresProcess)
	return c, nil
}

func (c *pgConnection) connect() error {
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

	dbname, err := mysqlHostSections[0].GetSingleValue("dbname", "")
	if err != nil {
		return err
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
	if err != nil {
		return err
	}

	// connect to postgres
	pgprocess, err := postgresutils.NewDB(host, port, dbname, user, password, "disable", pwdSections[0])
	if err != nil {
		return err
	}

	c.process = pgprocess

	fmt.Println("PG:", c)

	return nil
}

func (c *pgConnection) disconnect() error {
	fmt.Println("disconnect from PSQL")
	return nil
}

func (c *pgConnection) getSourceInfo(schemas []string) ([]schema, error) {
	return nil, nil
}

func (c *pgConnection) startDump() (<-chan interface{}, error) {
	return nil, nil
}

func (c *pgConnection) playEvent(e interface{}) error {
	return nil
}
