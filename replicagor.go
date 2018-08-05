package main

import (
	"flag"
	"fmt"
	//	"math/rand"
	"errors"
	"os"
	"os/signal"
	"syscall"
	//	"time"
	"strconv"
	"strings"

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
	blinfos []structs.BinLogInfo         // slice with information about binlog positions for latest event in every buffer
}

// Create replicator object, initialize source & destination, and create replication info
func NewReplicagor(rconf vconfig.VConfig, sconf vconfig.VConfig, logging *logrus.Logger) (*replicagor, error) {
	// create new replicagor object
	r := new(replicagor)

	// this is logger
	r.logging = logging

	r.logging.Infof("Initializing and connecting to source")
	source, err := NewConnection(SOURCE, sconf, rconf, logging)
	if err != nil {
		return nil, err
	}
	r.logging.Infof("Source connection is OK")
	r.source = source

	r.logging.Infof("Initializing and connecting to destination")
	dest, err := NewConnection(DEST, sconf, rconf, logging)
	if err != nil {
		return nil, err
	}
	r.logging.Infof("Destination connection is OK")
	r.dest = dest

	return r, nil

}

// run replication
func (r *replicagor) Run() error {
	stops := make(map[chan bool]structs.STOPCH)
	// create goroutines
	conts := make([]chan bool, 0)
	events := make([]chan *structs.Event, 0)

	// buffers
	freqs := r.source.getFreqs()
	blinfos := make([]structs.BinLogInfo, len(freqs))
	r.blinfos = blinfos
	for idf, f := range freqs {
		/* chan length shall be equal to number of buffers since
		  killing loop in goroutine aways sends signal to all buffers
		even the ones are already ended
		*/
		cont := make(chan bool, 2)
		conts = append(conts, cont)

		event := make(chan *structs.Event, 500) // get channel length from config
		events = append(events, event)

		//		bli := structs.BinLogInfo{Position: 0, File: ""}
		//		blinfos = append(blinfos, bli)

		stop := make(chan bool, 1)
		stopped := make(chan bool, 1)
		name := fmt.Sprintf("Buffer with frequency %v", f)

		stops[stop] = structs.STOPCH{Name: name, Stopped: stopped, Isbuff: true, BufCont: cont}
		//fmt.Println("freq:", freqs[idf], stop, stopped)

		go eventBuffer(idf, cont, event, r.dest, stop, stopped, blinfos)
	}

	// control routine
	stop_cr := make(chan bool, 1)
	stopped_cr := make(chan bool, 1)
	stops[stop_cr] = structs.STOPCH{Name: "Control", Stopped: stopped_cr, Isbuff: false}
	//fmt.Println("control:", stop_cr, stopped_cr)

	go r.controlRoutine(freqs, conts, stop_cr, stopped_cr)
	r.stops = stops

	// start dump and get event channel
	r.logging.Info("Starting binlogdump")
	stop_d := make(chan bool, 1) // channel to stop binlog routine
	stopped_d := make(chan bool, 1)
	stops[stop_d] = structs.STOPCH{Name: "Binlog Dump", Stopped: stopped_d, Isbuff: false}

	//fmt.Println("dump:", stop_d, stopped_d)

	stop_uri := make(chan bool, 1) // channel to stop update rinfor routine
	stopped_uri := make(chan bool, 1)
	stops[stop_uri] = structs.STOPCH{Name: "Update rinfo", Stopped: stopped_uri, Isbuff: false}

	//fmt.Println("uri:", stop_uri, stopped_uri)

	echan, err := r.source.startDump(stop_d, stopped_d, stop_uri, stopped_uri)
	if err != nil {
		r.logging.Errorf("Binlogdump cannot be started:%v Stopping replications", err)
		delete(stops, stop_d)
		delete(stops, stop_uri)
		r.stopAndExit()
		return err
	}

	// termination channel
	kill := make(chan os.Signal, 2)
	signal.Notify(kill, os.Interrupt, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM)

	// listen for all stopped channels. Once receive stopped signal initiate stopAndExit (via stopchan)
	stopchan := make(chan bool, 1)
	go func() {
		for {
			for stop, stopped := range r.stops {
				select {
				case <-stopped.Stopped:
					r.logging.Infof("%v routine has stopped. Will stop replication", stopped.Name)
					if !stopped.Isbuff {
						delete(stops, stop)
					}
					stopchan <- true
					return
				default:
				}
			}
		}
	}()

	// main cycle
	// to be moved to a separate routine

	//numes := 0
	//t1 := time.Now()

	for {
		select {
		case <-stopchan: // if one of the routines stopped
			//fmt.Println("stop received from chan")
			r.stopAndExit()
			return errors.New("Replicator exited due to stopped signal from goroutine")
		case k := <-kill:
			switch k {
			case syscall.SIGKILL, syscall.SIGINT, syscall.SIGTSTP, syscall.SIGTERM:
				r.logging.Info("Receive termination signal. Will stop replication")
				r.stopAndExit()
				return errors.New("Replication stopped due to a termination signal")
			}
		case event, ok := <-echan: // get source event or channel closure
			if ok {
				events[event.Buf] <- event
				//numes++
			} else { // when channel closed
				//fmt.Printf("done with %v events\n", numes)
			}
			//			if numes >= 160000 {
			//				fmt.Println(time.Now().Sub(t1).Nanoseconds()/1e6, "ms")
			//			}
		}

	}
	return nil
}

// Stops gorouines.
// Stops all routines except buffers. Waits for stopped signal.
// Disconnects from source and destination
func (r *replicagor) stopAndExit() {
	// first stop all non-buffer routines
	for stop, stopped := range r.stops {
		if !stopped.Isbuff {
			r.logging.Infof("Stopping routine %v", stopped.Name)
			stop <- true
			_, _ = <-stopped.Stopped
			r.logging.Infof("Routine %v stopped", stopped.Name)
		}
	}
	// then stop all buffer routines
	for stop, stopped := range r.stops {
		if stopped.Isbuff {
			r.logging.Infof("Stopping routine %v", stopped.Name)
			stop <- true
			stopped.BufCont <- true // send to continue channel so that stop signal can be caught by the buffer
			_, _ = <-stopped.Stopped
			r.logging.Infof("Routine %v stopped", stopped.Name)

		}
	}

	// finally write to sconfig latest valid binlog position and filename

	s, err := r.source.GetSConfig().GetSingleValue("binlog", "position", "")
	if err != nil {
		r.logging.Error(err)
		return
	}
	oldpos, _ := strconv.Atoi(s)

	oldfile, err := r.source.GetSConfig().GetSingleValue("binlog", "file", "")
	if err != nil {
		r.logging.Error(err)
		return
	}

	path, err := r.source.GetSConfig().GetSingleValue("File", "Path", "")
	if err != nil {
		r.logging.Error(err)
		return
	}

	pos, file := getsmallestPosition(r.blinfos, uint32(oldpos), oldfile)

	fmt.Println("position:", pos)
	fmt.Println("file:", file)

	blsec, err := r.source.GetSConfig().GetSections("binlog")
	if err != nil {
		r.logging.Error("Cannot save latest valid binlogposition: %v", err)
		return
	}

	blsec[0].SetValues("position", []string{fmt.Sprintf("%v", pos)})
	blsec[0].SetValues("file", []string{file})

	if err := r.source.GetSConfig().ToFile(path); err != nil {
		r.logging.Errorf("Cannot save latest valid binlogposition: %v", err)
		return
	}

	r.logging.Infof("Exited at %v binlogposition in %v", pos, file)

	// disconect from source and destination

}

func getsmallestPosition(blinfos []structs.BinLogInfo, defpos uint32, deffile string) (uint32, string) {
	smallestPositon := blinfos[0].Position
	smallestFile := blinfos[0].File

	var smallestFileNumber int

	if smallestFile == "" {
		smallestFileNumber = -1
	} else {
		smallestFileNumber, _ = strconv.Atoi(strings.Trim(strings.Split(smallestFile, ".")[1], "0"))
	}

	var pos uint32
	var fileNumber int
	var file string

	for _, blinfo := range blinfos {
		pos = blinfo.Position
		file = blinfo.File

		if file == "" {
			fileNumber = -1
		} else {
			fileNumber, _ = strconv.Atoi(strings.Trim(strings.Split(blinfo.File, ".")[1], "0"))
		}

		if (fileNumber < smallestFileNumber && fileNumber != -1) ||
			(fileNumber == smallestFileNumber && pos < smallestPositon && fileNumber != -1) ||
			(smallestFileNumber == -1 && fileNumber != -1) {
			smallestPositon = pos
			smallestFile = file
			smallestFileNumber = fileNumber
		}
	}

	if smallestPositon == 0 && smallestFile == "" {
		return uint32(defpos), deffile
	}
	return smallestPositon, smallestFile
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
	idf int,
	cont <-chan bool,
	event <-chan *structs.Event,
	dest connection,
	stop <-chan bool,
	stopped chan<- bool,
	blinfos []structs.BinLogInfo,
) {
	s := false
	for {
		<-cont // wait for signal from control routine
		select {
		case <-stop: // stop goroutine
			stopped <- true
			return
		default: // select below is under default (not another case) since
			// we want to catch stop signal with 100% certanty.
			// if event below is another case case <-stop will be caught
			// randomly and loop can be dead on waiting for an event
			select {
			case e := <-event:
				if !s {
					if err := dest.playEvent(e); err != nil {
						//fmt.Println("error while running query trying to stop replicagor")
						stopped <- true
						// continue cycle so it continues receiving events
						// and is killed from stop routine
						s = true
						//fmt.Println("buffer stopped")
					} else {
						if e.Position != 0 { // write position only if event has info about it
							blinfos[idf].Position = e.Position // write event's binlog position and filename
							blinfos[idf].File = e.File         // write event's binlog position and filename
						}
					}
				}

				//fmt.Println(idf, blinfos)
			default: // if no event on channel, then skip
				//time.Sleep(time.Second)
				//fmt.Println("waiting for event", stop)
			}
		}
	}
}

// control routine
func (r *replicagor) controlRoutine(
	freqs []int,
	conts []chan bool,
	stop <-chan bool,
	stopped chan<- bool,
	//	bstops map[chan bool]structs.STOPCH,
) {
	fcounter := make([]int, len(freqs)-1) // 0th element is default
	copy(fcounter, freqs[1:])             //exclude default buffer from fcounter

	for {
		select {
		case <-stop:
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
	fmt.Println("Starting Replicagor. For commandline help type replicagor --help")
	// read command line parameters
	cmdflags := new(cmdFlags)
	cmdflags.parseCmdFlags()

	// init logging
	fmt.Printf("Initializing logging into %v\n", cmdflags.logfile)
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
	logging.Infof("Getting info about resources from %v", cmdflags.rConfigFile)
	rconf, err := vconfig.New(cmdflags.rConfigFile, ",")
	if err != nil {
		logging.Error(err)
		return

	}

	logging.Infof("Getting info about source from %v", cmdflags.sConfigFile)
	sconf, err := vconfig.New(cmdflags.sConfigFile, ",")
	if err != nil {
		logging.Error(err)
		return
	}

	if ss, _ := sconf.GetSectionsByName("File"); len(ss) == 0 {
		fmt.Println("add file")
		s := vconfig.NewSection("File", ",")
		s.AddValues("Path", []string{cmdflags.sConfigFile})
		sconf.AddSection(s)
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
