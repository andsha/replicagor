package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type (
	schema struct {
		name   string
		buf    int
		freq   int
		tables []table
	}

	table struct {
		name                    string
		excludedFromReplication bool
		enableDelete            bool
		buf                     int
		freq                    int
		fields                  []field
	}

	field struct {
		name                    string
		excludedFromReplication bool
		isPKey                  bool
	}
)

/*   buffer numbers shall be integers starting from 0 in increasing order
without interruptions.
Frequences are how often event plays with respect to default event, whose
frequence is always 1 (play each time). e.g. buf/freq 0/1, 1/10, 2/100 means
1st buffer play events evety 10th time whereas 2nd buffer plays events
every 100th time.
Zeroeth buffer is default (no schema or table assigned to it).
Frequency of default buffer must be 1 (plays each time).
*/

func (r *replicagor) genRInfo() ([]int, error) {
	schemaSections, err := r.rconf.GetSectionsByName("replicatedDatabases")
	if err != nil {
		return nil, err
	}

	schemas, err := schemaSections[0].GetValues("databases")
	if err != nil {
		return nil, err
	}

	sinfo, err := r.source.getSourceInfo(schemas)
	if err != nil {
		return nil, err
	}

	rinfo := make([]schema, len(sinfo))
	copy(rinfo, sinfo)

	for idd := range rinfo {
		schema := &rinfo[idd]
		cfgschemaSections, _ := r.rconf.GetSectionsByVar("", "schema", schema.name)

		for _, cfgschemaSetion := range cfgschemaSections {
			switch cfgschemaSetion.Name() {
			case "excludedTables":
				cfgtables, err := cfgschemaSetion.GetValues("tables")
				if err != nil {
					return nil, err
				}
				for _, cfgtname := range cfgtables {
					for idt := range schema.tables {
						tab := &schema.tables[idt]
						if tab.name == cfgtname {
							tab.excludedFromReplication = true
							//fmt.Println(s.name, t)
						}
					}
				}
			case "excludedFields":
				cfgtables, err := cfgschemaSetion.GetValues("table")
				if err != nil {
					return nil, err
				}

				for idt := range schema.tables {
					tab := &schema.tables[idt]
					if tab.name == cfgtables[0] {
						cfgfields, err := cfgschemaSetion.GetValues("fields")
						if err != nil {
							return nil, err
						}
						for _, cfgf := range cfgfields {
							for idf := range tab.fields {
								fld := &tab.fields[idf]
								if fld.name == cfgf {
									fld.excludedFromReplication = true
								}
							}
						}
					}
				}

			case "enableDelete":
				cfgtables, err := cfgschemaSetion.GetValues("tables")
				if err != nil {
					return nil, err
				}
				for _, cfgtname := range cfgtables {
					for idt := range schema.tables {
						tab := &schema.tables[idt]
						if tab.name == cfgtname {
							tab.enableDelete = true
							//fmt.Println(s.name, t)
						}
					}
				}
			}
		}
	}

	bufferSections, err := r.rconf.GetSectionsByName("buffer")
	if err != nil {
		return nil, err
	}

	bufs := make([][]int, 0)

	for _, bufsec := range bufferSections {
		num, err := bufsec.GetSingleValue("number", "")
		if err != nil {
			return nil, errors.New(fmt.Sprintf("%v. number is a required field in buffer section", err))
		}
		numint, err := strconv.Atoi(num)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error while converting number to int. %v", err))
		}
		freq, err := bufsec.GetSingleValue("frequency", "")
		if err != nil {
			return nil, errors.New(fmt.Sprintf("%v. frequency is a required field in buffer section", err))
		}
		freqint, err := strconv.Atoi(freq)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error while converting frequency to int. %v", err))
		}
		b := []int{numint, freqint}
		bufs = sintappend(bufs, b, 0)

		schemas, _ := bufsec.GetValues("schemas")
		tables, _ := bufsec.GetValues("tables")

		if len(schemas) != 0 {
			for _, schema := range schemas {
				for ids, s := range rinfo {
					if schema == s.name {
						rinfo[ids].buf = numint
						rinfo[ids].freq = freqint
					}
				}
			}
		} else if len(tables) != 0 {
			for _, stable := range tables {
				tvar := strings.Split(stable, ".")
				schema := tvar[0]
				table := tvar[1]
				for ids, s := range rinfo {
					if schema == s.name {
						for idt, t := range s.tables {
							if table == t.name {
								rinfo[ids].tables[idt].buf = numint
								rinfo[ids].tables[idt].freq = freqint
							}
						}
					}
				}
			}

		} else {
			for ids := range rinfo {
				if rinfo[ids].buf == 0 {
					rinfo[ids].freq = freqint
				}
			}
		}
	}

	if bufs[len(bufs)-1][0] > len(bufs)-1 {
		return nil, errors.New(fmt.Sprintf("Buffer numbers must not have missing numbers and shall start from 0. [buf:freq]: %v", bufs))
	}

	if bufs[0][1] != 1 {
		return nil, errors.New(fmt.Sprintf("Frequency of default (0'th) buffer shall be 1"))
	}

	r.rinfo = rinfo

	freqs := func(s [][]int) []int {
		a := make([]int, 0)
		for _, v := range s {
			a = append(a, v[1])
		}
		return a
	}(bufs)

	return freqs, nil
}

func (r *replicagor) putToBuffer(event interface{}, events []chan interface{}) error {

	return nil
}

func sintappend(sslice [][]int, aslice []int, pos int) [][]int {
	for ids, s := range sslice {
		if aslice[pos] < s[pos] {
			sslice = append(sslice, aslice)
			copy(sslice[ids+1:], sslice[ids:])
			sslice[ids] = aslice
			return sslice
		}
	}
	sslice = append(sslice, aslice)
	return sslice
}

// buffer routine for treating events
func eventBuffer(cont <-chan bool, event <-chan interface{}, dest connection) {

	<-cont

	select {
	case e := <-event:
		dest.playEvent(e)
	default:
	}
}

// control routine
func controlRoutine(freqs []int, conts []chan bool) {
	fcounter := make([]int, len(freqs)-1) // 0th element is default
	copy(fcounter, freqs[1:])             //exclude default buffer from fcounter

	for {
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
