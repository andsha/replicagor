package main

import (
	"flag"
	"fmt"
	//	"math/rand"
	"os"
	//	"time"
	"errors"

	"github.com/andsha/vconfig"
	"github.com/sirupsen/logrus"
)

type cmdFlags struct {
	verbose     bool
	rConfigFile string
	sConfigFile string
	logfile     string
}

//parse command line flags
func (flags *cmdFlags) parseCmdFlags() {
	flag.BoolVar(&flags.verbose, "verbose", false, "Show more output")
	flag.StringVar(&flags.rConfigFile, "rConfig", "", "path to the file with configuration for schemas, tables, and fields")
	flag.StringVar(&flags.sConfigFile, "sConfig", "", "path to the file with configuration for source")
	flag.StringVar(&flags.logfile, "logfile", "/tmp/log.log", "Where to write logs")
	flag.Parse()
}

// main class
type replicagor struct {
	logging *logrus.Logger   //logging
	source  connection       // replication master (copy from)
	dest    connection       // replication slave (copy to)
	rinfo   []schema         //replication info
	rconf   *vconfig.VConfig //replication config
}

// Create replicator object, initialize source & destination, and sreate replication info
func NewReplicagor(rConfigFile string, sConfigFile string, logging *logrus.Logger) (*replicagor, []int, error) {
	// create new replicagor object
	r := new(replicagor)

	// this is logger
	r.logging = logging

	// replication config
	rconf, err := vconfig.New(rConfigFile)
	if err != nil {
		return nil, nil, err
	}
	r.rconf = &rconf

	// search for source connection info
	sconf, err := vconfig.New(sConfigFile)
	if err != nil {
		return nil, nil, err
	}

	var sourceconf *vconfig.VConfig
	if _, err := sconf.GetSectionsByName("source"); err != nil {
		if _, err := rconf.GetSectionsByName("source"); err != nil {
			return nil, nil, errors.New(fmt.Sprintf("Cannot find section source in %v or %v", rConfigFile, sConfigFile))
		} else {
			sourceconf = &rconf
		}
	} else {
		sourceconf = &sconf
	}

	r.logging.Infof("Initializing source")

	source, err := NewConnection(SOURCE, sourceconf)
	if err != nil {
		return nil, nil, err
	}
	r.source = source

	r.logging.Info("Connecting to source")
	if err := r.source.connect(); err != nil {
		return nil, nil, err
	}

	// search for destination connection info
	var destconf *vconfig.VConfig
	if _, err := rconf.GetSectionsByName("destination"); err != nil {
		if _, err := sconf.GetSectionsByName("destination"); err != nil {
			return nil, nil, errors.New(fmt.Sprintf("Cannot find section destination in %v or %v", rConfigFile, sConfigFile))
		} else {
			destconf = &sconf
		}
	} else {
		destconf = &rconf
	}

	r.logging.Infof("Initializing destination")

	dest, err := NewConnection(DEST, destconf)
	if err != nil {
		return nil, nil, err
	}
	r.dest = dest

	r.logging.Info("Connecting to destination")
	if err := r.dest.connect(); err != nil {
		return nil, nil, err
	}

	// create rinfo
	r.logging.Info("Creating replication info structure")
	numbufs, err := r.genRInfo()
	if err != nil {
		return nil, nil, err
	}

	return r, numbufs, nil

}

// run replication
func (r *replicagor) Run(freqs []int) error {
	// create goroutines
	conts := make([]chan bool, 0)
	events := make([]chan interface{}, 0)

	for _ = range freqs {
		cont := make(chan bool, 1)
		conts = append(conts, cont)

		event := make(chan interface{}, 500) // get channel length from config
		events = append(events, event)

		go eventBuffer(cont, event, r.dest)
	}

	// control routine
	go controlRoutine(freqs, conts)

	// start dump and get event channel
	echan, err := r.source.startDump()
	if err != nil {
		return err
	}

	// main cycle
	// to be moved to a separate routine
	numes := 0
	for {
		event, ok := <-echan // get event from source or channel closure
		if ok {
			r.putToBuffer(event, events)
			numes++
		} else { // when channel closed
			fmt.Printf("done with %v events\n", numes)
		}
	}
	return nil
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
	// set default logging level
	if cmdflags.verbose {
		logging.SetLevel(logrus.DebugLevel)
	} else {
		logging.SetLevel(logrus.InfoLevel)
	}

	// Crate instance of replicagor
	myreplication, freqs, err := NewReplicagor(cmdflags.rConfigFile, cmdflags.sConfigFile, logging)
	if err != nil {
		logging.Error(err)
		return
	}

	logging.Info("Start Replication")

	// start replication
	if err := myreplication.Run(freqs); err != nil {
		logging.Error(err)
		return
	}

	logging.Info("Finish Replication")
}
