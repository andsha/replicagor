package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/andsha/executebashcmd"
	"github.com/andsha/postgresutils"
)

func TestReplicagor(t *testing.T) {
	// create folder for Postgres server
	errCount := 0
	defer func() {
		//remove test folder, we should remove everything only if test successfull
		fmt.Println(fmt.Sprintf(" Test is done with %v errors ", errCount))
	}()
	currentDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Run test from", currentDir)

	_ = os.RemoveAll(fmt.Sprintf("%v/test/", currentDir))

	// *************************  Postgres ***************

	// create postgres test folder
	if err := os.MkdirAll(fmt.Sprintf("%v/test/pg", currentDir), 0700); err != nil {
		t.Fatal(err)
	}
	defer func() {
		//remove test folder, we should remove everything only if test successfull
		fmt.Println("deleting folder")
		if errCount == 0 {
			if err := os.RemoveAll(fmt.Sprintf("%v/test/", currentDir)); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// create postgres server; run initdb
	fmt.Println("Creating Postgres DB")
	cmd := fmt.Sprintf("initdb -D %v/test/pg -U testuser", currentDir)
	if err := exec.Command("sh", "-c", cmd).Run(); err != nil {
		t.Fatal(err)
	}

	//
	pfile, err := ioutil.ReadFile(fmt.Sprintf("%v/test/pg/postgresql.conf", currentDir))
	if err != nil {
		t.Fatal(err)
	}

	out := fmt.Sprintf("%v\nport = 1196\ntimezone = '+00:00'", string(pfile))

	if err := ioutil.WriteFile(fmt.Sprintf("%v/test/pg/postgresql.conf", currentDir), []byte(out), 0700); err != nil {
		t.Fatal(err)
	}

	// start server
	fmt.Println("Starting Postgres Server")
	cmd = fmt.Sprintf("pg_ctl -D %v/test/pg -U testuser start", currentDir)
	if err := exec.Command("sh", "-c", cmd).Run(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)

	defer func() {
		// stop server
		cmd = fmt.Sprintf("pg_ctl -D %v/test/pg stop -m i", currentDir)
		fmt.Println("stopping postgres")
		if err := exec.Command("sh", "-c", cmd).Run(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second * 5)

	}()

	//create db
	fmt.Println("Create Postgres DB")
	cmd = "createdb -p 1196 testdb -U testuser"
	out, errout, err := executebashcmd.ExecuteBashCmd(cmd, nil)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("out:", out, "errout:", errout, "err:", err)
	//if err := exec.Command("sh", "-c", cmd).Run(); err != nil {fmt.Println(err);t.Fatalf("%v", err)}

	// connect to postgres
	fmt.Println("Connect to Posgres DB")
	pgconn, err := postgresutils.NewDB("127.0.0.1", "1196", "testdb", "testuser", "", "disable", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		fmt.Println("closing connection to postgres")
		pgconn.CloseDB()
	}()

	// ***********************  Mysql ********************

	// create Mysql test folder
	if err := os.MkdirAll(fmt.Sprintf("%v/test/mysql", currentDir), 0700); err != nil {
		t.Fatal(err)
	}
	//	defer func() {
	//		//remove test folder, we should remove everything only if test successfull
	//		if errCount == 0 {
	//			if err := os.RemoveAll(fmt.Sprintf("%v/test/", currentDir)); err != nil {
	//				errCount += 1
	//				t.Fatal(err)
	//			}
	//		}
	//	}()

	//create config string
	conf := createMysql57ConStr(currentDir)
	if err := ioutil.WriteFile(fmt.Sprintf("%v/test/mysql57.conf", currentDir), []byte(conf), 0700); err != nil {
		t.Fatal(err)
	}

	// create mysql server; run initdb
	fmt.Println("Creating Mysql57 DB")
	if err := os.MkdirAll(fmt.Sprintf("%v/test/mysql/log", currentDir), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := os.OpenFile(fmt.Sprintf("%v/test/mysql/log/mysqld.log", currentDir), os.O_RDONLY|os.O_CREATE, 0700); err != nil {
		errCount += 1
		t.Fatal(err)
	}
	if _, err := os.OpenFile(fmt.Sprintf("%v/test/mysql/errmsg.sys", currentDir), os.O_RDONLY|os.O_CREATE, 0700); err != nil {
		errCount += 1
		t.Fatal(err)
	}
	cmd = fmt.Sprintf(`/usr/local/mysql-5.7.20-macos10.12-x86_64/bin/mysqld --defaults-file=%v/test/mysql57.conf --initialize-insecure`, currentDir)
	fmt.Println(cmd)
	if err := exec.Command("sh", "-c", cmd).Run(); err != nil {
		fmt.Println("err:", err)
		errCount += 1
		t.Fatal(err)
	}

}

func createMysql57ConStr(currDir string) string {
	conf := fmt.Sprintf(`[mysqld]
basedir=%v/test/mysql
datadir=%v/test/mysql/data
socket=%v/test/mysql/mysql.sock
port=1296
bind-address=0.0.0.0
innodb
innodb_file_per_table
interactive_timeout=900
server-id=100
secure-file-priv =
### Disable performance_schema engine ###
performance_schema=0

tmpdir                          = /tmp/

skip_external_locking

default_storage_engine          = innodb

character_set_server            = utf8
collation_server                = utf8_general_ci

max_connections                 = 500
max_connect_errors              = 999999

thread_cache_size               = 32
table_open_cache                = 4096

query_cache_type                = 1
query_cache_size                = 32M

thread_stack                    = 192K

max_allowed_packet              = 1M
sort_buffer_size                = 2M

tmp_table_size                  = 32M
max_heap_table_size             = 64M

# we start in read-only mode,
# either we are in standby datacenter
# or we wait for manual (flipper) intervention to go RW.
read_only                       = 1

# we don't start replication at startup, so we don't polute the database
# with data we don't necessarely want.
skip_slave_start                = 1

# Slow Query Logging
long_query_time                 = 1
slow_query_log                  = 1
slow_query_log_file             = mysql-slow-0.log

# only keep 60 days of master logs
expire_logs_days                = 7

# MyISAM
key_buffer_size                 = 32M
myisam_sort_buffer_size         = 64M
read_buffer_size                = 2M
read_rnd_buffer_size            = 8M

# InnoDB
#innodb_data_home_dir            =
#innodb_data_file_path           = ibdata1:10M:autoextend
#innodb_autoextend_increment     = 8

innodb_buffer_pool_size         = 10M

#innodb_log_group_home_dir       = /data/logs/
innodb_log_files_in_group       = 2
innodb_log_file_size            = 5242880
innodb_log_buffer_size          = 64M

innodb_lock_wait_timeout        = 50

innodb_flush_log_at_trx_commit  = 1
innodb_flush_method             = O_DIRECT
innodb_thread_concurrency       = 16
innodb_max_dirty_pages_pct      = 90
innodb_max_purge_lag            = 0
innodb_adaptive_hash_index      = ON
innodb_autoinc_lock_mode = 0

# use ROW binlog format in MySQL 5.1
binlog-format                   = ROW
log-bin = mysql-bin

log_bin_trust_function_creators =1
pid-file                        = %v/test/mysql/mysqld.pid
log-error                       = %v/test/mysql/log/mysqld.log

[mysql]
no_auto_rehash
default_character_set           = latin1

[mysqld_safe]
log-error=%v/test/mysql/log/mysqld.log
pid-file=%v/test/mysql/mysqld.pid

[client]
socket=%v/test/mysql/mysql.sock
port=1297

[mysqld]
sql_mode = "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
default_password_lifetime       = 0
lc_messages_dir = %v/test/mysql
lc_messages = en_US`, currDir, currDir, currDir, currDir, currDir, currDir, currDir, currDir, currDir)
	return conf
}
