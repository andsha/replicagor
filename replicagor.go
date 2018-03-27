package main

import (
	"flag"
	"fmt"
	//	"math/rand"
	"errors"
	"os"
	"time"

	"github.com/andsha/replicagor/structs"
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
	flag.StringVar(&flags.rConfigFile, "rconfig", "", "path to the file with configuration for schemas, tables, and fields")
	flag.StringVar(&flags.sConfigFile, "sconfig", "", "path to the file with configuration for source")
	flag.StringVar(&flags.logfile, "logfile", "/tmp/log.log", "Where to write logs")
	flag.Parse()
}

// main class
type replicagor struct {
	logging *logrus.Logger               //logging
	source  connection                   // replication master (copy from)
	dest    connection                   // replication slave (copy to)
	stops   map[chan bool]structs.STOPCH // map of channels: key is channel to stop routine. value is whether the routine stopped

}

// Create replicator object, initialize source & destination, and create replication info
func NewReplicagor(rconf vconfig.VConfig, sconf vconfig.VConfig, logging *logrus.Logger) (*replicagor, error) {
	// create new replicagor object
	r := new(replicagor)

	// this is logger
	r.logging = logging

	r.logging.Infof("Initializing and connecting to source")
	source, err := NewConnection(SOURCE, sconf, rconf)
	if err != nil {
		return nil, err
	}
	r.source = source

	r.logging.Infof("Initializing and connecting to destination")
	dest, err := NewConnection(DEST, sconf, rconf)
	if err != nil {
		return nil, err
	}
	r.dest = dest

	return r, nil

}

// run replication
func (r *replicagor) Run() error {
	// map: [channel to stop] : [channel is stopped, is buffer  routine]
	stops := make(map[chan bool]structs.STOPCH)
	// create goroutines
	conts := make([]chan bool, 0)
	events := make([]chan *structs.Event, 0)

	bstops := make(map[chan bool]structs.STOPCH)
	freqs := r.source.getFreqs()
	for _ = range freqs {
		/* chan length shall be equal to number of buffers since
		  killing loop in goroutine aways sends signal to all buffers
		even the ones are already ended
		*/
		cont := make(chan bool, len(freqs))
		conts = append(conts, cont)

		event := make(chan *structs.Event, 500) // get channel length from config
		events = append(events, event)

		stop := make(chan bool, 1)
		stopped := make(chan bool, 1)

		stops[stop] = structs.STOPCH{Stopped: stopped, Isbuff: true}
		bstops[stop] = structs.STOPCH{Stopped: stopped, Isbuff: true}
		//fmt.Println("freq:", freqs[idf], stop, stopped)

		go eventBuffer(cont, event, r.dest, stop, stopped)
	}

	// control routine
	stop_cr := make(chan bool, 1)
	stopped_cr := make(chan bool, 1)
	stops[stop_cr] = structs.STOPCH{Stopped: stopped_cr, Isbuff: false}
	//fmt.Println("control:", stop_cr, stopped_cr)

	go controlRoutine(freqs, conts, stop_cr, stopped_cr, bstops)
	r.stops = stops

	// start dump and get event channel
	stop_d := make(chan bool, 1) // channel to stop binlog routine
	stopped_d := make(chan bool, 1)
	stops[stop_d] = structs.STOPCH{Stopped: stopped_d, Isbuff: false}

	//fmt.Println("dump:", stop_d, stopped_d)

	stop_uri := make(chan bool, 1) // channel to stop update rinfor routine
	stopped_uri := make(chan bool, 1)
	stops[stop_uri] = structs.STOPCH{Stopped: stopped_uri, Isbuff: false}

	//fmt.Println("uri:", stop_uri, stopped_uri)

	echan, err := r.source.startDump(stop_d, stopped_d, stop_uri, stopped_uri)
	if err != nil {
		r.stopAndExit()
		return err
	}

	r.stops = stops

	// main cycle
	// to be moved to a separate routine
	numes := 0
	t1 := time.Now()

	// listen for all stopped channels. Once receive stopped signal initiate stopAndExit (via stopchan)
	stopchan := make(chan bool, 1)
	go func() {
		for {
			for _, stopped := range r.stops {
				select {
				case s := <-stopped.Stopped:
					//fmt.Println("caught stop signal. Exit")
					stopchan <- s
					return
				default:
				}
			}
		}
	}()

	for {
		select {
		case <-stopchan: // if one of the routines stopped
			//fmt.Println("stop received from chan")
			r.stopAndExit()
			return errors.New("Replicator exited due to stopped signal from goroutine")
		case event, ok := <-echan: // get source event or channel closure
			if ok {
				events[event.Buf] <- event
				numes++
			} else { // when channel closed
				fmt.Printf("done with %v events\n", numes)
			}
			if numes >= 160000 {
				fmt.Println(time.Now().Sub(t1).Nanoseconds()/1e6, "ms")
			}
		}

	}
	return nil
}

// stops all gorouines. waits for their execution. Disconnects from source and destination
func (r *replicagor) stopAndExit() {
	for stop, stopped := range r.stops {
		if !stopped.Isbuff {
			//fmt.Println("closing", stop, stopped)
			stop <- true
			//fmt.Println("stop sent")
			_, _ = <-stopped.Stopped
			//fmt.Println("stopped received")
		}
	}

	// disconect from source and destination

}

/*   buffer numbers shall be integers starting from 0 in increasing order
without interruptions. Zeroeth buffer is default; no schema or table shall be assigned to it.
Frequences are how often event plays with respect to default event, whose
frequence is always 1 (play each time). e.g. buf/freq 0/1, 1/10, 2/100 means
1st buffer play events evety 10th time whereas 2nd buffer plays events
every 100th time.
Frequency of default buffer must be 1 (plays each time).
*/

// buffer routine for treating events
// routine can only be stopped from control routine
func eventBuffer(
	cont <-chan bool,
	event <-chan *structs.Event,
	dest connection,
	stop <-chan bool,
	stopped chan<- bool,
) {
	for {
		<-cont // wait for signal from control routine
		select {
		case e := <-event:
			dest.playEvent(e)
		case <-stop: // stop goroutine
			//fmt.Println("buffer routine stopped")
			stopped <- true
			return
		default: // if no event on channel, then skip
			//time.Sleep(time.Second)
			//fmt.Println("waiting for event", stop)
		}
	}
}

// control routine
func controlRoutine(
	freqs []int,
	conts []chan bool,
	stop <-chan bool,
	stopped chan<- bool,
	bstops map[chan bool]structs.STOPCH,
) {
	fcounter := make([]int, len(freqs)-1) // 0th element is default
	copy(fcounter, freqs[1:])             //exclude default buffer from fcounter

	for {
		select {
		case <-stop:
			//fmt.Println("receive signal to stop control routne. Will stop buffer routines", stop, stopped)
			for bstop, bstopped := range bstops {
				//fmt.Printf("stopping buffer %v\n", bstop)
				bstop <- true             // send stop signal to buffer
				for _, c := range conts { // send cont signal to all buffers to play all still alive
					c <- true
				}
				//fmt.Println("waiting for buffer to stop")
				<-bstopped.Stopped
				//fmt.Println("buffer stopped. Continue")
			}
			//fmt.Println("All buffers are stopped")
			stopped <- true
			return
		default:
			for idf := range fcounter {
				fcounter[idf]--
				if fcounter[idf] == 0 {
					conts[idf+1] <- true // send to non-default buffer based on its frequency
					fcounter[idf] = freqs[idf+1]
				}
			}
			conts[0] <- true // always send to default buffer
		}
	}
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

	rconf, err := vconfig.New(cmdflags.rConfigFile, ",")
	if err != nil {
		logging.Error(err)
		return
	}

	sconf, err := vconfig.New(cmdflags.sConfigFile, ",")
	if err != nil {
		logging.Error(err)
		return
	}

	myreplication, err := NewReplicagor(rconf, sconf, logging)
	if err != nil {
		logging.Error(err)
		return
	}

	logging.Info("Start Replication")

	// start replication
	if err := myreplication.Run(); err != nil {
		logging.Error(err)
		return
	}

	logging.Info("Finish Replication")
}
