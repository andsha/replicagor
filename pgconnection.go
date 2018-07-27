// postgres connection implementation

package main

import (
	//	"errors"
	"fmt"
	//	"strconv"
	//	"time"

	"github.com/andsha/postgresutils"
	"github.com/andsha/replicagor/pgfuncs"
	"github.com/andsha/replicagor/structs"
	"github.com/andsha/vconfig"
)

// extends conn
type pgConnection struct {
	*conn
	process *postgresutils.PostgresProcess
}

func NewPgConnection(c *conn) (*pgConnection, error) {
	pgc := new(pgConnection)
	pgc.conn = c
	pgc.process = new(postgresutils.PostgresProcess)

	// connect to postgres
	if err := pgc.blconnect(); err != nil {
		return nil, err
	}
	return pgc, nil
}

func (c *pgConnection) GetSConfig() *vconfig.VConfig {
	return &c.sconf
}

func (c *pgConnection) blconnect() error {
	credentials, err := c.getConnCredentials()
	if err != nil {
		return err
	}
	var pwc vconfig.VConfig
	if err := pwc.FromString(credentials["SECURE PASSWORD"], ","); err != nil {
		return err
	}
	pws, err := pwc.GetSectionsByName("SECURE PASSWORD")
	pgprocess, err := postgresutils.NewDB(
		credentials["host"],
		credentials["port"],
		credentials["dbname"],
		credentials["user"],
		credentials["password"],
		"disable",
		pws[0])
	if err != nil {
		return err
	}

	c.process = pgprocess

	return nil
}

func (c *pgConnection) sqlconnect() error {
	return nil
}

func (c *pgConnection) getConnCredentials() (map[string]string, error) {
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

	if dbname, err := sec.GetSingleValue("dbname", ""); err != nil {
		return nil, err
	} else {
		credentials["dbname"] = dbname
	}

	if user, err := sec.GetSingleValue("user", ""); err != nil {
		return nil, err
	} else {
		credentials["user"] = user
	}

	if password, err := sec.GetSingleValue("password", ""); err != nil {
		return nil, err
	} else {
		credentials["password"] = password
	}

	if pwdSections, err := cfg.GetSections("SECURE PASSWORD"); err != nil {
		return nil, err
	} else {
		credentials["SECURE PASSWORD"] = pwdSections[0].ToString()
	}

	return credentials, nil
}

func (c *pgConnection) disconnect() error {
	//fmt.Println("disconnect from PSQL")
	return nil
}

func (c *pgConnection) startDump(stop_d <-chan bool,
	stopped_d chan<- bool,
	stop_uri <-chan bool,
	stopped_uri chan<- bool) (<-chan *structs.Event, error) {
	return nil, nil
}

func (c *pgConnection) getFreqs() []int {
	return c.freqs
}

func (c *pgConnection) playEvent(e *structs.Event) error {
	query := e.Query
	if len(query) == 0 {
		if q, err := pgfuncs.GenQuery(e); err != nil {
			return err
		} else {
			query = q
		}
	} else {
		if q, err := pgfuncs.ConvertMysql57ToPostgres(query, true); err != nil {
			return err
		} else {
			query = q
		}
	}

	res, err := c.process.Run(query)
	fmt.Println(query, res, err)
	if err != nil {
		return err
	}

	return nil
}
