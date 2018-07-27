package structs

const (
	DELETE_EVENT byte = 0
	INSERT_EVENT byte = 1
	UPDATE_EVENT byte = 2
)

type (
	Schema struct {
		Name   string
		Buf    int
		Freq   int
		Tables []*Table
	}

	Table struct {
		Name                    string
		ExcludedFromReplication bool
		EnableDelete            bool
		Buf                     int
		Freq                    int
		Columns                 []*Column
	}

	Column struct {
		Name                    string
		Type                    string
		Enum                    []string
		ExcludedFromReplication bool
		IsPKey                  bool
	}

	Event struct {
		TableName  string // shall be byte?
		SchemaName string // shall be byte?
		Columns    []*Column
		EventType  byte
		Query      string
		OldValues  [][]*QueryValues
		NewValues  [][]*QueryValues
		Buf        int
		Position   uint32 // binlig position
		File       string // binlog file
	}

	BinLogInfo struct {
		Position uint32 // binlig position
		File     string // binlog file
	}

	QueryValues struct {
		ColumnId int
		Value    interface{}
	}

	Record struct {
		Keys   []string
		Values []string
	}

	ST struct {
		Schema string
		Table  string
	}

	STOPCH struct {
		Name    string
		Stopped chan bool
		Isbuff  bool
		BufCont chan bool
	}

	EVCHAN struct {
		Ev  interface{}
		Err error
	}
)
