package mysqlconnection

import (
	//	"encoding/binary"
	//	"errors"
	"fmt"
	"math"
	//	"strings"
	//	"strconv"
	//	"time"
	//	"sync"

	"github.com/andsha/replicagor/structs"
)

type (
	EventLog struct {
		mysqlConnection *MysqlProcess
		binlogVersion   uint16

		lastRotatePosition uint32
		lastRotateFileName string

		headerQueryEventLength        byte
		headerDeleteRowsEventV1Length byte
		headerUpdateRowsEventV1Length byte
		headerWriteRowsEventV1Length  byte

		lastTableMapEvent *TableMapEvent

		additionalLength int

		eventChan chan *structs.Event
	}

	eventLogHeader struct {
		Timestamp    uint32
		EventType    byte
		ServerId     uint32
		EventSize    uint32
		NextPosition uint32
		Flags        uint16
	}

	logRotateEvent struct {
		*eventLogHeader
		position       uint64
		binlogFileName []byte
	}

	formatDescriptionEvent struct {
		*eventLogHeader
		binlogVersion          uint16
		mysqlServerVersion     []byte
		createTimestamp        uint32
		eventTypeHeaderLengths []byte
	}

	startEventV3Event struct {
		*eventLogHeader
		binlogVersion      uint16
		mysqlServerVersion []byte
		createTimestamp    uint32
	}

	QueryEvent struct {
		*eventLogHeader
		slaveProxyId  uint32
		executionTime uint32
		errorCode     uint16
		statusVars    []byte
		schema        string
		query         string
		binLogVersion uint16
	}

	XidEvent struct {
		*eventLogHeader
		TransactionId uint64
	}

	IntVarEvent struct {
		*eventLogHeader
		_type byte
		value uint64
	}

	BeginLoadQueryEvent struct {
		*eventLogHeader
		fileId    uint32
		blockData string
	}

	ExecuteLoadQueryEvent struct {
		*eventLogHeader
		slaveProxyId     uint32
		executionTime    uint32
		errorCode        uint16
		statusVars       []byte
		schema           string
		fileId           uint32
		startPos         uint32
		endPos           uint32
		dupHandlingFlags byte
		query            string
	}

	UserVarEvent struct {
		*eventLogHeader
		name    string
		isNil   bool
		_type   byte
		charset uint32
		value   string
		flags   byte
	}

	IncidentEvent struct {
		*eventLogHeader
		Type    uint16
		Message string
	}

	RandEvent struct {
		*eventLogHeader
		seed1 uint64
		seed2 uint64
	}

	TableMapEvent struct {
		*eventLogHeader
		TableId    uint64
		Flags      uint16
		SchemaName string
		TableName  string
		Columns    []*TableMapEventColumn
	}

	TableMapEventColumn struct {
		Type     byte
		MetaInfo []byte
		Nullable bool
	}

	rowsEvent struct {
		*eventLogHeader
		tableMapEvent    *TableMapEvent
		postHeaderLength byte

		tableId   uint64
		flags     uint16
		extraData []byte
		values    [][]*structs.QueryValues
		newValues [][]*structs.QueryValues
	}

	//	RowsEventValue struct {
	//		columnId int
	//		//isNull   bool
	//		value interface{}
	//		//_type    byte
	//	}

	DeleteEvent struct {
		*rowsEvent
	}

	WriteEvent struct {
		*rowsEvent
	}

	UpdateEvent struct {
		*rowsEvent
	}

	unknownEvent struct {
		*eventLogHeader
	}

	binLogEvent interface {
		read(*pack)
	}

	AppendBlockEvent struct {
		*BeginLoadQueryEvent
	}

	StopEvent struct {
		*unknownEvent
	}

	slaveEvent struct {
		*unknownEvent
	}

	ignorableEvent struct {
		*unknownEvent
	}

	HeartBeatEvent struct {
		*unknownEvent
	}
)

//func (event *RowsEventValue) GetType() byte {
//	return event._type
//}

//func (event *structs.QueryValues) GetValue() interface{} {
//	return event.Value
//}

//func (event *RowsEventValue) IsNil() bool {
//	return event.isNull
//}

//func (event *structs.QueryValues) GetColumnId() int {
//	return event.ColumnId
//}

func (event *rowsEvent) GetSchema() string {
	return event.tableMapEvent.SchemaName
}

func (event *rowsEvent) GetTable() string {
	return event.tableMapEvent.TableName
}

func (event *rowsEvent) GetRows() [][]*structs.QueryValues {
	return event.values
}

func (event *UpdateEvent) GetNewRows() [][]*structs.QueryValues {
	return event.newValues
}

func isTrue(columnId int, bitmap []byte) bool {
	return (bitmap[columnId/8]>>uint8(columnId%8))&1 == 1
}

func (event *rowsEvent) read(pack *pack) {
	//fmt.Println("EVENTE:", event)

	isUpdateEvent := event.EventType == _UPDATE_ROWS_EVENTv1 || event.EventType == _UPDATE_ROWS_EVENTv2

	if event.postHeaderLength == 6 {
		var tableId uint32
		pack.readUint32(&tableId)
		event.tableId = uint64(tableId)
	} else {
		pack.readSixByteUint64(&event.tableId)
	}

	pack.readUint16(&event.Flags)

	//If row event == 2
	if event.EventType >= _WRITE_ROWS_EVENTv2 && event.EventType <= _DELETE_ROWS_EVENTv2 {
		var extraDataLength uint16
		pack.readUint16(&extraDataLength)
		extraDataLength -= 2
		event.extraData = pack.Next(int(extraDataLength))
	}

	var (
		columnCount uint64
		isNull      bool
	)

	pack.readIntLengthOrNil(&columnCount, &isNull)
	bitMapLength := int((columnCount + 7) / 8)

	var columnPreset, columnPresentBitmap1, columnPresentBitmap2, nullBitmap []byte

	columnPresentBitmap1 = pack.Next(bitMapLength)
	if isUpdateEvent {
		columnPresentBitmap2 = pack.Next(bitMapLength)
	}

	event.values = [][]*structs.QueryValues{}
	event.newValues = [][]*structs.QueryValues{}

	switcher := true

	for {
		nullBitmap = pack.Next(bitMapLength)

		row := []*structs.QueryValues{}
		for i, column := range event.tableMapEvent.Columns {

			if switcher {
				columnPreset = columnPresentBitmap1
			} else {
				columnPreset = columnPresentBitmap2
			}

			if !isTrue(i, columnPreset) {
				continue
			}

			value := &structs.QueryValues{
				ColumnId: i,
				//_type:    column.Type,
			}
			//fmt.Println("column type:", column.Type)

			if isTrue(i, nullBitmap) {
				value.Value = nil
				//value.isNull = true
			} else {
				switch column.Type {
				case MYSQL_TYPE_ENUM,
					MYSQL_TYPE_SET, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_BLOB,
					MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_BIT:
					value.Value, _ = pack.readStringLength()
				case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING: //MYSQL_TYPE_STRING
					val, _ := pack.readStringLength()
					//fmt.Println("string val:", val)
					value.Value = string(val)
				case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
					//fmt.Println("decimal precision:", int(column.MetaInfo[0]), "scale:", int(column.MetaInfo[1]))
					value.Value = pack.readNewDecimal(int(column.MetaInfo[0]), int(column.MetaInfo[1]))
				case MYSQL_TYPE_LONGLONG:
					var val uint64
					pack.readUint64(&val)
					value.Value = val
				case MYSQL_TYPE_LONG:
					var val uint32
					pack.readUint32(&val)
					value.Value = val
				case MYSQL_TYPE_INT24:
					var val uint32
					pack.readThreeByteUint32(&val)
					value.Value = val
				case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
					var val uint16
					pack.readUint16(&val)
					value.Value = val
				case MYSQL_TYPE_TINY, MYSQL_TYPE_STRING: // string is correct type for enum
					//fmt.Println("column:", string(column.MetaInfo[1]))
					value.Value, _ = pack.ReadByte()
					//fmt.Println(value.value)
				case MYSQL_TYPE_FLOAT:
					var val uint32
					pack.readUint32(&val)
					value.Value = float32(math.Float32frombits(val))
				case MYSQL_TYPE_DOUBLE:
					var val uint64
					pack.readUint64(&val)
					value.Value = math.Float64frombits(val)
				case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
					value.Value = pack.readDateTime()
				case MYSQL_TYPE_TIMESTAMP2:
					value.Value = pack.readTimeStamp2()
				case MYSQL_TYPE_DATETIME2:
					value.Value = pack.readDateTime2()
				case MYSQL_TYPE_TIME:
					value.Value = pack.readTime()
				}
			}
			row = append(row, value)
		}

		if switcher {
			event.values = append(event.values, row)
		} else {
			event.newValues = append(event.newValues, row)
		}

		if isUpdateEvent {
			switcher = !switcher
		}

		if pack.Len() == 0 {
			break
		}
	}
}

func (event *TableMapEvent) read(pack *pack) {
	pack.readSixByteUint64(&event.TableId)
	pack.readUint16(&event.Flags)

	schemaLength, _ := pack.ReadByte()
	event.SchemaName = string(pack.Next(int(schemaLength)))
	filler, _ := pack.ReadByte()
	if filler != 0 {
		panic("incorrect filler")
	}

	tableLength, _ := pack.ReadByte()
	event.TableName = string(pack.Next(int(tableLength)))
	filler, _ = pack.ReadByte()
	if filler != 0 {
		panic("incorrect filler")
	}

	var columnCount uint64
	var isNull bool

	pack.readIntLengthOrNil(&columnCount, &isNull)

	columnTypeDef := pack.Next(int(columnCount))
	columnMetaDef, _ := pack.readStringLength()
	columnNullBitMap := pack.Bytes()
	event.Columns = make([]*TableMapEventColumn, columnCount)

	metaOffset := 0

	for i := 0; i < len(columnTypeDef); i++ {
		column := &TableMapEventColumn{
			Type:     columnTypeDef[i],
			Nullable: (columnNullBitMap[i/8]>>uint8(i%8))&1 == 1,
		}

		switch columnTypeDef[i] {
		case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_DECIMAL,
			MYSQL_TYPE_NEWDECIMAL, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
			column.MetaInfo = columnMetaDef[metaOffset : metaOffset+2]
			metaOffset += 2
		case MYSQL_TYPE_BLOB, MYSQL_TYPE_DOUBLE, MYSQL_TYPE_FLOAT, MYSQL_TYPE_TIMESTAMP2, MYSQL_TYPE_DATETIME2:
			column.MetaInfo = columnMetaDef[metaOffset : metaOffset+1]
			metaOffset += 1
		default:
			column.MetaInfo = []byte{}
		}

		event.Columns[i] = column
	}
}

func (event *RandEvent) GetSeed1() uint64 {
	return event.seed1
}

func (event *RandEvent) GetSeed2() uint64 {
	return event.seed2
}

func (event *RandEvent) read(pack *pack) {
	pack.readUint64(&event.seed1)
	pack.readUint64(&event.seed2)
}

func (event *IncidentEvent) read(pack *pack) {
	pack.readUint16(&event.Type)
	length, _ := pack.ReadByte()
	event.Message = string(pack.Next(int(length)))
}

func (event *unknownEvent) read(pack *pack) {

}

func (event *UserVarEvent) GetName() string {
	return event.name
}

func (event *UserVarEvent) GetType() byte {
	return event._type
}

func (event *UserVarEvent) IsNil() bool {
	return event.isNil
}

func (event *UserVarEvent) GetCharset() uint32 {
	return event.charset
}

func (event *UserVarEvent) GetValue() string {
	return event.value
}

func (event *UserVarEvent) read(pack *pack) {
	var nameLength uint32
	pack.readUint32(&nameLength)
	event.name = string(pack.Next(int(nameLength)))
	isNull, _ := pack.ReadByte()
	event.isNil = isNull == 1
	if event.isNil {
		return
	}

	event._type, _ = pack.ReadByte()
	pack.readUint32(&event.charset)
	var length uint32
	pack.readUint32(&length)
	event.value = string(pack.Next(int(length)))
	event.flags, _ = pack.ReadByte()
}

func (event *ExecuteLoadQueryEvent) GetSchema() string {
	return event.schema
}

func (event *ExecuteLoadQueryEvent) GetQuery() string {
	return event.query
}

func (event *ExecuteLoadQueryEvent) GetExecutionTime() uint32 {
	return event.executionTime
}

func (event *ExecuteLoadQueryEvent) GetErrorCode() uint16 {
	return event.errorCode
}

func (event *ExecuteLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.slaveProxyId)
	pack.readUint32(&event.executionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.errorCode)

	var statusVarsLength uint16
	pack.readUint16(&statusVarsLength)

	pack.readUint32(&event.fileId)
	pack.readUint32(&event.startPos)
	pack.readUint32(&event.endPos)
	event.dupHandlingFlags, _ = pack.ReadByte()

	event.statusVars = pack.Next(int(statusVarsLength))
	event.schema = string(pack.Next(int(schemaLength)))

	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog EXECUTE_LOAD_QUERY_EVENT structure")
	}

	event.query = string(pack.Bytes())
}

func (event *BeginLoadQueryEvent) GetData() string {
	return event.blockData
}

func (event *BeginLoadQueryEvent) read(pack *pack) {
	pack.readUint32(&event.fileId)
	event.blockData = string(pack.Bytes())
}

func (event *IntVarEvent) GetValue() uint64 {
	return event.value
}

func (event *IntVarEvent) GetType() byte {
	return event._type
}

func (event *IntVarEvent) read(pack *pack) {
	event._type, _ = pack.ReadByte()
	pack.readUint64(&event.value)
}

func (event *XidEvent) read(pack *pack) {
	pack.readUint64(&event.TransactionId)
}

func (event *QueryEvent) GetQuery() string {
	return event.query
}

func (event *QueryEvent) GetExecutionTime() uint32 {
	return event.executionTime
}

func (event *QueryEvent) GetErrorCode() uint16 {
	return event.errorCode
}

func (event *QueryEvent) GetSchema() string {
	return event.schema
}

func (event *QueryEvent) read(pack *pack) {
	pack.readUint32(&event.slaveProxyId)
	pack.readUint32(&event.executionTime)

	schemaLength, _ := pack.ReadByte()

	pack.readUint16(&event.errorCode)

	if event.binLogVersion >= 4 {
		var statusVarsLength uint16
		pack.readUint16(&statusVarsLength)
		event.statusVars = pack.Next(int(statusVarsLength))
	}

	event.schema = string(pack.Next(int(schemaLength)))
	splitter, _ := pack.ReadByte()

	if splitter != 0 {
		panic("Incorrect binlog QUERY_EVENT structure")
	}

	event.query = string(pack.Bytes())
}

func (event *logRotateEvent) read(pack *pack) {
	pack.readUint64(&event.position)
	event.binlogFileName = pack.Next(pack.Len())
}

func (event *formatDescriptionEvent) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = pack.Next(50)
	pack.readUint32(&event.createTimestamp)
	length, _ := pack.ReadByte()
	event.eventTypeHeaderLengths = pack.Next(int(length))
}

func (event *startEventV3Event) read(pack *pack) {
	pack.readUint16(&event.binlogVersion)
	event.mysqlServerVersion = make([]byte, 50)
	pack.Read(event.mysqlServerVersion)

	pack.readUint32(&event.createTimestamp)
}

func (eh *eventLogHeader) readHead(pack *pack) {
	pack.ReadByte()
	pack.readUint32(&eh.Timestamp)
	eh.EventType, _ = pack.ReadByte()
	pack.readUint32(&eh.ServerId)
	pack.readUint32(&eh.EventSize)
	pack.readUint32(&eh.NextPosition)
	pack.readUint16(&eh.Flags)
}

func newEventLog(mysqlConnection *MysqlProcess, additionalLength int) *EventLog {
	el := EventLog{}
	el.mysqlConnection = mysqlConnection
	el.eventChan = make(chan *structs.Event, 1)
	el.additionalLength = additionalLength
	return &el
}

func (ev *EventLog) GetLastPosition() uint32 {
	return ev.lastRotatePosition
}

func (ev *EventLog) GetLastLogFileName() string {
	return string(ev.lastRotateFileName)
}

func (ev *EventLog) GetEventChan() <-chan *structs.Event {
	return ev.eventChan
}

// main loop to listen events from binlog
func (evlog *EventLog) Start(stop <-chan bool, stopped chan<- bool, startPos uint32) {
	replicateEv := true
	deleteEv := false
	buffer := 0
	tab := new(structs.Table)
	var columns []*structs.Column
	//t := 0
	listencont := make(chan bool, 1)

	evChan := make(chan structs.EVCHAN, 1)
	go func() {
		//fmt.Println("start event loop")
		for {
			<-listencont
			ev, err := evlog.readEvent()
			//fmt.Println("event!")
			evChan <- structs.EVCHAN{Ev: ev, Err: err}
		}
	}()
	listencont <- true

	pos := startPos // current binlog position

	for {
		select {
		case <-stop:
			//fmt.Println("dump received stop signal")
			stopped <- true
			return
		case ec := <-evChan:
			//fmt.Println("event")
			//fmt.Println(evlog.lastRotatePosition, evlog.lastRotateFileName)
			if ec.Err != nil {
				evlog.mysqlConnection.logging.Errorf("Error while reading binlog:%v", ec.Err)

				// check what kind of error. if connection error then try reconnecting N times. after that exit routine

				//if false { // this emulates sending  indication of error to the main thread
				stopped <- true
				//}
				listencont <- true
				return
			}
			//fmt.Printf("event type: %T\n", ec.Ev)
			pos = evlog.lastRotatePosition

			switch e := ec.Ev.(type) {
			case *startEventV3Event: // ?
				//fmt.Println("1.1")
				evlog.binlogVersion = e.binlogVersion
			case *formatDescriptionEvent:
				//fmt.Println("1.2")
				evlog.binlogVersion = e.binlogVersion
				evlog.headerQueryEventLength = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_QUERY_POSITION]

				evlog.headerDeleteRowsEventV1Length = 8
				evlog.headerUpdateRowsEventV1Length = 8
				evlog.headerWriteRowsEventV1Length = 8

				if len(e.eventTypeHeaderLengths) >= 24 {
					evlog.headerDeleteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_DELETEV1_POSITION]
					evlog.headerUpdateRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_UPDATEV1_POSITION]
					evlog.headerWriteRowsEventV1Length = e.eventTypeHeaderLengths[_FORMAT_DESCRIPTION_LENGTH_WRITEV1_POSITION]
				}
			case *logRotateEvent:
				//fmt.Println("1.3")
				evlog.lastRotateFileName = string(e.binlogFileName)
			case *XidEvent: // COMMIT query
				event := new(structs.Event)
				event.Query = "COMMIT"
				event.Position = pos
				event.File = evlog.lastRotateFileName
				evlog.eventChan <- event
			case *QueryEvent:
				for _, s := range evlog.mysqlConnection.rinfo {
					if s.Name == e.schema {
						event := new(structs.Event)
						q := e.GetQuery()
						//fmt.Println("Query:'", q, "'", len(q))
						if q != "BEGIN;" && q != "BEGIN" && q != " BEGIN" && q != " BEGIN;" {
							//fmt.Println("update DBinfo", event.Query)
							_ = evlog.mysqlConnection.UpdateDBinfo(e.schema, "")
							q = fmt.Sprintf("SET SEARCH_PATH TO \"%v\"; %v", e.schema, q)
							event.Position = pos
							event.File = evlog.lastRotateFileName
						}
						event.Query = q
						evlog.eventChan <- event
					}
				}
			case *TableMapEvent:
				fmt.Println("TableMapEvent. Schema:", e.SchemaName, "Table:", e.TableName)
				//fmt.Println("2")
				evlog.lastTableMapEvent = e
				replicateEv = false
				deleteEv = false
				columns = nil

				for _, s := range evlog.mysqlConnection.rinfo {
					//fmt.Println("TableMapEvent:", e.SchemaName, "s:", s.Name)
					if e.SchemaName == s.Name {
						buffer = s.Buf
						replicateEv = true
						foundTable := false
						tab = nil

						for _, t := range s.Tables {
							if e.TableName == t.Name {
								foundTable = true
								tab = t
								columns = t.Columns
								break
							}
						}

						if !foundTable {
							t := evlog.mysqlConnection.UpdateDBinfo(e.SchemaName, e.TableName)
							if t == nil {
								// write error to log
							} else {
								foundTable = true
								tab = t
								columns = t.Columns
							}
						}

						if foundTable { // tab != nil
							buffer = tab.Buf
							if tab.ExcludedFromReplication {
								replicateEv = false
							}
							if tab.EnableDelete {
								deleteEv = true
							}
						}
						break
					}
				}

			case *rowsEvent:
				//fmt.Println("3")
				if !replicateEv {
					listencont <- true
					continue
				}

				event := new(structs.Event)
				event.SchemaName = evlog.lastTableMapEvent.SchemaName
				event.TableName = evlog.lastTableMapEvent.TableName
				event.Columns = columns
				event.Buf = buffer
				//event.Position = evlog.lastRotatePosition
				//event.File = evlog.lastRotateFileName
				switch e.EventType {
				case _DELETE_ROWS_EVENTv0, _DELETE_ROWS_EVENTv1, _DELETE_ROWS_EVENTv2:
					//fmt.Println("delete event?", deleteEv)
					if !deleteEv {
						listencont <- true
						continue
					}
					event.EventType = structs.DELETE_EVENT
				case _UPDATE_ROWS_EVENTv0, _UPDATE_ROWS_EVENTv1, _UPDATE_ROWS_EVENTv2:
					event.EventType = structs.UPDATE_EVENT
					event.NewValues = e.newValues
				case _WRITE_ROWS_EVENTv0, _WRITE_ROWS_EVENTv1, _WRITE_ROWS_EVENTv2:
					event.EventType = structs.INSERT_EVENT
				}
				event.OldValues = e.values
				evlog.eventChan <- event
				//				if t >= 1 {
				//					stopped <- true
				//					close(stopped)
				//					listencont <- true
				//					return
				//				}
				//				t++

			default:
				//fmt.Println("4")
			}
			//fmt.Println("5")
			//pos = evlog.lastRotatePosition // current position is next read position
			listencont <- true
		default:
		}
	}
}

func (ev *EventLog) readEvent() (interface{}, error) {
	pack, err := ev.mysqlConnection.packReader.readNextPackWithAdditionalLength(ev.additionalLength)

	if err != nil {
		return nil, err
	}

	header := &eventLogHeader{}
	header.readHead(pack)

	err = pack.isError()

	if err != nil {
		return nil, err
	}

	var event binLogEvent

	switch header.EventType {
	case _START_EVENT_V3:
		event = &startEventV3Event{
			eventLogHeader: header,
		}
	case _FORMAT_DESCRIPTION_EVENT:
		event = &formatDescriptionEvent{
			eventLogHeader: header,
		}
	case _ROTATE_EVENT:
		event = &logRotateEvent{
			eventLogHeader: header,
		}
	case _QUERY_EVENT:
		event = &QueryEvent{
			eventLogHeader: header,
			binLogVersion:  ev.binlogVersion,
		}
	case _XID_EVENT:
		event = &XidEvent{
			eventLogHeader: header,
		}
	case _INTVAR_EVENT:
		event = &IntVarEvent{
			eventLogHeader: header,
		}
	case _BEGIN_LOAD_QUERY_EVENT:
		event = &BeginLoadQueryEvent{
			eventLogHeader: header,
		}
	case _APPEND_BLOCK_EVENT:
		event = &AppendBlockEvent{
			&BeginLoadQueryEvent{
				eventLogHeader: header,
			},
		}
	case _EXECUTE_LOAD_QUERY_EVENT:
		event = &ExecuteLoadQueryEvent{
			eventLogHeader: header,
		}
	case _USER_VAR_EVENT:
		event = &UserVarEvent{
			eventLogHeader: header,
		}
	case _UNKNOWN_EVENT:
		event = &unknownEvent{
			eventLogHeader: header,
		}
	case _IGNORABLE_EVENT:
		event = &ignorableEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _HEARTBEAT_EVENT:
		event = &HeartBeatEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _STOP_EVENT:
		event = &StopEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _INCIDENT_EVENT:
		event = &IncidentEvent{
			eventLogHeader: header,
		}
	case _SLAVE_EVENT:
		event = &slaveEvent{
			&unknownEvent{
				eventLogHeader: header,
			},
		}
	case _RAND_EVENT:
		event = &RandEvent{
			eventLogHeader: header,
		}
	case _TABLE_MAP_EVENT:
		event = &TableMapEvent{
			eventLogHeader: header,
		}
	case _DELETE_ROWS_EVENTv0:
		fallthrough
	case _DELETE_ROWS_EVENTv1:
		fallthrough
	case _DELETE_ROWS_EVENTv2:
		fallthrough
	case _UPDATE_ROWS_EVENTv0:
		fallthrough
	case _UPDATE_ROWS_EVENTv1:
		fallthrough
	case _UPDATE_ROWS_EVENTv2:
		fallthrough
	case _WRITE_ROWS_EVENTv0:
		fallthrough
	case _WRITE_ROWS_EVENTv1:
		fallthrough
	case _WRITE_ROWS_EVENTv2:
		event = &rowsEvent{
			eventLogHeader:   header,
			postHeaderLength: ev.headerWriteRowsEventV1Length,
			tableMapEvent:    ev.lastTableMapEvent,
		}
	default:
		//		println("Unknown event")
		//		println(fmt.Sprintf("% x\n", pack.buff))
		return nil, nil
	}

	ev.lastRotatePosition = header.NextPosition
	event.read(pack)

	return event, nil
}
