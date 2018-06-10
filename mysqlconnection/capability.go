package mysqlconnection

//http://dev.mysql.com/doc/internals/en/capability-flags.html
const (
	_CLIENT_LONG_PASSWORD                  uint32 = 0x00000001
	_CLIENT_FOUND_ROWS                     uint32 = 0x00000002
	_CLIENT_LONG_FLAG                      uint32 = 0x00000004
	_CLIENT_CONNECT_WITH_DB                uint32 = 0x00000008
	_CLIENT_NO_SCHEMA                      uint32 = 0x00000010
	_CLIENT_COMPRESS                       uint32 = 0x00000020
	_CLIENT_ODBC                           uint32 = 0x00000040
	_CLIENT_LOCAL_FILES                    uint32 = 0x00000080
	_CLIENT_IGNORE_SPACE                   uint32 = 0x00000100
	_CLIENT_PROTOCOL_41                    uint32 = 0x00000200
	_CLIENT_INTERACTIVE                    uint32 = 0x00000400
	_CLIENT_SSL                            uint32 = 0x00000800
	_CLIENT_IGNORE_SIGPIPE                 uint32 = 0x00001000
	_CLIENT_TRANSACTIONS                   uint32 = 0x00002000
	_CLIENT_RESERVED                       uint32 = 0x00004000
	_CLIENT_SECURE_CONNECTION              uint32 = 0x00008000
	_CLIENT_MULTI_STATEMENTS               uint32 = 0x00010000
	_CLIENT_MULTI_RESULTS                  uint32 = 0x00020000
	_CLIENT_PS_MULTI_RESULTS               uint32 = 0x00040000
	_CLIENT_PLUGIN_AUTH                    uint32 = 0x00080000
	_CLIENT_CONNECT_ATTRS                  uint32 = 0x00100000
	_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA uint32 = 0x00200000
	_CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS   uint32 = 0x00400000
	_CLIENT_SESSION_TRACK                  uint32 = 0x00800000
	_CLIENT_DEPRECATE_EOF                  uint32 = 0x01000000

	_HANDSHAKE_VERSION_10 = 10

	_MAX_PACK_SIZE uint32 = 16*1024*1024 - 1

	_CLIENT_ALL_FLAGS = _CLIENT_LONG_PASSWORD |
		_CLIENT_FOUND_ROWS |
		_CLIENT_LONG_FLAG |
		_CLIENT_NO_SCHEMA |
		_CLIENT_ODBC |
		_CLIENT_LOCAL_FILES |
		_CLIENT_IGNORE_SPACE |
		_CLIENT_PROTOCOL_41 |
		_CLIENT_INTERACTIVE |
		_CLIENT_IGNORE_SIGPIPE |
		_CLIENT_TRANSACTIONS |
		_CLIENT_RESERVED |
		_CLIENT_SECURE_CONNECTION |
		_CLIENT_MULTI_STATEMENTS |
		_CLIENT_MULTI_RESULTS
)
