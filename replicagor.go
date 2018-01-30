package main

import (
	"flag"
	"fmt"
	"os"

	//"github.com/2tvenom/myreplication"
	"github.com/andsha/vconfig"
	"github.com/sirupsen/logrus"
)

type cmdFlags struct {
	verbose bool
	config  string
	logfile string
}

//parse command line flags
func (flags *cmdFlags) parseCmdFlags() {
	flag.BoolVar(&flags.verbose, "verbose", false, "Show more output")
	flag.StringVar(&flags.config, "config", "", "path to the file with configuration")
	flag.StringVar(&flags.logfile, "logfile", "/tmp/log.log", "Where to write logs")
	flag.Parse()
}

// this struct has all replicator's functionality
type replicagor struct {
	// main configuration struct
	config *vconfig.VConfig

	//logging
	logging *logrus.Logger

	// replication master (copy from)
	source connection

	// replication slave (copy to)
	dest connection

	// array of schemas to keep information about tables
	tableInfo []schema

	//array to hold event queues. indexed by priority
	queues []queue
}

// initialize replicator
func (r *replicagor) Init() error {
	// initialize source
	srcCfg, err := r.config.GetSections("source")
	if err != nil {
		return nil
	}
	if s, err := NewConnection(srcCfg[0], r); err != nil {
		return err
	} else {
		r.source = s
	}

	// initialize destination
	destCfg, err := r.config.GetSections("destination")
	if err != nil {
		return nil
	}
	if d, err := NewConnection(destCfg[0], r); err != nil {
		return err
	} else {
		r.dest = d
	}

	r.source.connect()
	r.dest.connect()

	// initialize queues
	t := make([]queue, 3)
	r.queues = t

	return nil
}

// Create replicator object
func NewReplicagor(p string, logging *logrus.Logger) (*replicagor, error) {
	// create new replicagor object
	r := new(replicagor)

	// this is logger
	r.logging = logging

	//reading config file
	r.logging.Infof(fmt.Sprintf("reading config from %v", p))

	if c, err := vconfig.New(p); err != nil {
		return nil, err
	} else {
		r.config = &c
	}

	return r, nil

}

func main() {
	// read command line parameters
	cmdflags := new(cmdFlags)
	cmdflags.parseCmdFlags()

	// init logging
	var logging = logrus.New()
	logfile, err := os.OpenFile(cmdflags.logfile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	defer logfile.Close()
	logging.Out = logfile
	// set default level
	if cmdflags.verbose {
		logging.SetLevel(logrus.DebugLevel)
	} else {
		logging.SetLevel(logrus.InfoLevel)
	}

	logging.Info("Start Replicagor")

	// Crate instance of replicagor
	myreplication, err := NewReplicagor(cmdflags.config, logging)
	if err != nil {
		logging.Error(err)
		return
	}

	if err := myreplication.Init(); err != nil {
		logging.Error(err)
		return
	}

	logging.Info("Finish Replcagor")
}
