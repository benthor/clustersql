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

import "testing"
import "github.com/go-sql-driver/mysql"
import "database/sql"

var db *sql.DB

func TestOpen(t *testing.T) {
	d := NewDriver(mysql.MySQLDriver{})
	
	d.AddNode("maria1", "root@tcp(127.0.0.1:3301)/test")
	d.AddNode("maria2", "root@tcp(127.0.0.1:3302)/test")
	d.AddNode("maria3", "root@tcp(127.0.0.1:3303)/test")
	d.AddNode("intentionallyBroken", "root@tcp(127.0.0.1:3304)/test")
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
