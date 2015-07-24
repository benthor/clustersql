// Copyright 2014 by tkr@ecix.net (Peering GmbH)
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright notice,
// this list of conditions and the following disclaimer in the documentation
// and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

package clustersql

import (
	"database/sql"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/go-sql-driver/mysql"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

var db *sql.DB

type NodeCfg struct {
	Name     string
	HostName string
	Port     int
	UserName string
	Password string
	DBName   string
}

type Config struct {
	Nodes []NodeCfg
}

func TestOpen(t *testing.T) {
	cfgfile := os.Getenv("DBCONFIG")
	if cfgfile == "" {
		cfgfile = "config.toml"
	}
	cfg := new(Config)
	if f, err := os.Open(cfgfile); err != nil {
		t.Fatal(err, "(did you set the DBCONFIG env variable?)")
	} else {
		if _, err := toml.DecodeReader(f, cfg); err != nil {
			t.Fatal(err)
		}
	}

	d := NewDriver("ClusterSql", mysql.MySQLDriver{})

	for _, ncfg := range cfg.Nodes {
		if ncfg.Password != "" {
			d.AddNode(ncfg.Name, fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", ncfg.UserName, ncfg.Password, ncfg.HostName, ncfg.Port, ncfg.DBName))
		} else {
			d.AddNode(ncfg.Name, fmt.Sprintf("%s@tcp(%s:%d)/%s", ncfg.UserName, ncfg.HostName, ncfg.Port, ncfg.DBName))
		}
	}

	sql.Register("cluster", d)
	var err error
	db, err = sql.Open("cluster", "galera")
	if err != nil {
		t.Error(err)
	}
}

func TestDrop(t *testing.T) {
	_, err := db.Exec("DROP TABLE IF EXISTS test")
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestCreate(t *testing.T) {
	_, err := db.Exec("CREATE TABLE test (value BOOL)")
	if err != nil {
		t.Fatalf(err.Error())
	}
}

func TestEmptySelect(t *testing.T) {
	rows, err := db.Query("SELECT * FROM test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if rows.Next() {
		t.Fatalf("unexpected data in empty table")
	}
}

func TestInsert(t *testing.T) {
	res, err := db.Exec("INSERT INTO test VALUES (1)")
	if err != nil {
		t.Fatalf(err.Error())
	}
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}
	id, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("res.LastInsertId() returned error: %s", err.Error())
	}
	if id != 0 {
		t.Fatalf("Expected InsertID 0, got %d", id)
	}
}

func TestSelect(t *testing.T) {
	rows, err := db.Query("SELECT value FROM test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if rows.Next() {
		var out bool
		rows.Scan(&out)
		if true != out {
			t.Errorf("true != %t", out)
		}
		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}
}

func TestUpdate(t *testing.T) {
	res, err := db.Exec("UPDATE test SET value = ? WHERE value = ?", false, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}

	rows, err := db.Query("SELECT value FROM test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	if rows.Next() {
		var out bool
		rows.Scan(&out)
		if false != out {
			t.Errorf("false != %t", out)
		}

		if rows.Next() {
			t.Error("unexpected data")
		}
	} else {
		t.Error("no data")
	}
}

func TestDelete(t *testing.T) {
	res, err := db.Exec("DELETE FROM test WHERE value = ?", false)
	if err != nil {
		t.Fatalf(err.Error())
	}
	count, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 1 {
		t.Fatalf("Expected 1 affected row, got %d", count)
	}

	// Check for unexpected rows
	res, err = db.Exec("DELETE FROM test")
	if err != nil {
		t.Fatalf(err.Error())
	}
	count, err = res.RowsAffected()
	if err != nil {
		t.Fatalf("res.RowsAffected() returned error: %s", err.Error())
	}
	if count != 0 {
		t.Fatalf("Expected 0 affected row, got %d", count)
	}
}

func TestConcurrent(t *testing.T) {
	var max int
	err := db.QueryRow("SELECT @@max_connections").Scan(&max)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	// max = 100
	fmt.Printf("Testing up to %d concurrent connections \r\n", max)
	var remaining, succeeded int32 = int32(max), 0

	var wg sync.WaitGroup
	wg.Add(max)
	var fatalError string
	var once sync.Once
	fatalf := func(s string, vals ...interface{}) {
		once.Do(func() {
			fatalError = fmt.Sprintf(s, vals...)
		})
	}

	for i := 0; i < max; i++ {
		go func(id int) {
			defer wg.Done()

			tx, err := db.Begin()
			atomic.AddInt32(&remaining, -1)
			fmt.Printf("%d ", remaining)

			if err != nil {
				if err.Error() != "Error 1040: Too many connections" {
					//fmt.Printf("Begin: Error on Conn %d: %s", id, err.Error())
					fatalf("Begin: Error on Conn %d: %s", id, err.Error())
					// t.Logf("Begin: Error on Conn %d: %s", id, err.Error())
				}
				//fmt.Printf(" whoops ")
				return
			}

			// keep the connection busy until all connections are open
			for remaining > 0 {
				if _, err = tx.Exec("DO 1"); err != nil {
					//fmt.Printf("Exec: Error on Conn %d: %s", id, err.Error())
					fatalf("Exec: Error on Conn %d: %s", id, err.Error())
					return
				}
			}

			if err = tx.Commit(); err != nil {
				//fmt.Printf("Error on Conn %d: %s", id, err.Error())
				fatalf("Error on Conn %d: %s", id, err.Error())
				return
			}

			// everything went fine with this connection
			atomic.AddInt32(&succeeded, 1)
		}(i)
	}

	fmt.Println("waiting")
	// wait until all conections are open
	wg.Wait()
	fmt.Println("waited")

	if fatalError != "" {
		t.Fatal(fatalError)
	}

	t.Logf("Reached %d concurrent connections\r\n", succeeded)

}
