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

// Package clustersql is an SQL "meta"-Driver - A clustering, implementation-
// agnostic wrapper for any backend implementing "database/sql/driver".
//
// It does (latency-based) load-balancing and error-recovery over all registered
// nodes.
//
// It is assumed that database-state is transparently replicated over all
// nodes by some database-side clustering solution. This driver ONLY handles
// the client side of such a cluster.
//
// All errors which are made non-fatal because of failover are logged.
//
// To make use of clustering, use clustersql with any backend driver
// implementing "database/sql/driver" like so:
//
//  import "database/sql"
//  import "github.com/go-sql-driver/mysql"
//  import "github.com/benthor/clustersql"
//
// There is currently no way around instanciating the backend driver explicitly
//
//  mysqlDriver := mysql.MySQLDriver{}
//
// You can perform backend-driver specific settings such as
//
//  err := mysql.SetLogger(mylogger)
//
// Create a new clustering driver with the backend driver
//
//	clusterDriver := clustersql.NewDriver(mysqlDriver)
//
// Add nodes, including driver-specific name format, in this case Go-MySQL DSN.
// Here, we add three nodes belonging to a galera (https://mariadb.com/kb/en/mariadb/documentation/replication-cluster-multi-master/galera/) cluster
//
//	clusterDriver.AddNode("galera1", "user:password@tcp(dbhost1:3306)/db")
//	clusterDriver.AddNode("galera2", "user:password@tcp(dbhost2:3306)/db")
//	clusterDriver.AddNode("galera3", "user:password@tcp(dbhost3:3306)/db")
//
// Make the clusterDriver available to the go sql interface under an arbitrary
// name
//
//	sql.Register("myCluster", clusterDriver)
//
// Open the registered clusterDriver with an arbitrary DSN string (not used)
//
//	db, err := sql.Open("myCluster", "whatever")
//
// Continue to use the sql interface as documented at
// http://golang.org/pkg/database/sql/
package clustersql

import (
	"database/sql/driver"
	"log"
)

type Driver struct {
	nodes          []node
	upstreamDriver driver.Driver
}

type node struct {
	Name string
	DSN  string
}

// AddNode registers a new DSN as name with the upstream Driver.
func (d *Driver) AddNode(name, DSN string) {
	d.nodes = append(d.nodes, node{name, DSN}) //, nil, false, nil})
}

// Open will be called by sql.Open once registered. The name argument is ignored (it is only there to satisfy the driver interface)
func (d Driver) Open(name string) (driver.Conn, error) {
	type c struct {
		name string
		conn driver.Conn
		err  error
	}
	die := make(chan bool)
	cc := make(chan c)
	for _, node := range d.nodes {
		go func(name, DSN string, cc chan c, die chan bool) {
			conn, err := d.upstreamDriver.Open(DSN)
			select {
			case cc <- c{name, conn, err}:
				//log.Println("selected", node.Name)
			case <-die:
				if conn != nil {
					conn.Close()
				}
			}
		}(node.Name, node.DSN, cc, die)
	}
	var n c
	// n = <-cc
	// close(die)
	for i := 0; i < len(d.nodes); i++ {
		n = <-cc
		if n.err == nil {
			close(die)
			break
		} else {
			log.Println(n.name, n.err)
			if n.conn != nil {
				n.conn.Close()
			}
		}
	}
	return n.conn, n.err
}

// NewDriver returns an initialized Cluster driver, using upstreamDriver as backend
func NewDriver(upstreamDriver driver.Driver) Driver {
	cl := Driver{[]node{}, upstreamDriver}
	return cl
}
