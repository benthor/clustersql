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
// It does (latency-based) load-balancing and error-recovery over the registered
// set of nodes.
//
// It is assumed that database-state is transparently replicated over all
// nodes by some database-side clustering solution. This driver ONLY handles
// the client side of such a cluster.
//
// This package simply multiplexes the driver.Open() function of sql/driver to every attached node. The function is called on each node, returning the first successfully opened connection. (Any connections opening subsequently will be closed.) If opening does not succeed for any node, the latest error gets returned. Any other errors will be masked by default. However, any given latest error for any attached node will remain exposed through expvar, as well as some basic counters and timestamps.
//
// To make use of this kind of clustering, use this package with any backend driver
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
//
// Before using this in production, you should configure your cluster details in config.toml and run
//
//  go test -v .
//
// Note however, that non-failure of the above is no guarantee for a correctly set-up cluster.
//
// Finally, you SHOULD set db.MaxIdleConns and db.MaxOpenConns to a non-zero value. Although the sql
// driver usually does a good job of doing its own pooling, file descriptors can leak in corner cases
// (of which this library might constitue an example).
package clustersql

import (
	"database/sql/driver"
	"expvar"
	"sort"
	"time"
)

type Driver struct {
	nodes          map[string]*node
	upstreamDriver driver.Driver
	exp            *expvar.Map
}

type node struct {
	Name string
	DSN  string
	exp  *expvar.Map
}

// AddNode registers a new DSN as name with the upstream Driver.
func (d *Driver) AddNode(name, DSN string) {
	m := new(expvar.Map).Init()
	n := node{name, DSN, m}
	d.exp.Set(name, m)
	d.nodes[name] = &n
}

// DelNode unregisters a named Node from the upstream Driver. This SHOULD(TM) be non-invasive, allowing all pending SQL actions on that node to complete as expected
func (d *Driver) DelNode(name string) {
	d.nodes[name] = nil
}

// Nodes returns a sorted list of the names of the registered Nodes.
func (d *Driver) Nodes() []string {
	var list []string
	for name := range d.nodes {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

// Open will be called by sql.Open once registered. The name argument is ignored (it is only there to satisfy the driver interface)
func (d Driver) Open(name string) (driver.Conn, error) {
	type c struct {
		conn driver.Conn
		err  error
		n    *node
	}
	die := make(chan bool)
	cc := make(chan c)
	for _, n := range d.nodes {
		go func(n *node, cc chan c, die chan bool) {
			conn, err := d.upstreamDriver.Open(n.DSN)
			select {
			case cc <- c{conn, err, n}:
				//log.Println("selected", node.Name)
			case <-die:
				if conn != nil {
					conn.Close()
				}
			}
		}(n, cc, die)
	}
	var n c
	for i := 0; i < len(d.nodes); i++ {
		Time := new(expvar.String)
		n = <-cc
		Time.Set(time.Now().String())
		if n.err == nil {
			n.n.exp.Add("Connections", 1)
			n.n.exp.Set("LastSuccess", Time)
			close(die)
			break
		} else {
			Err := new(expvar.String)
			Err.Set(n.err.Error())
			n.n.exp.Add("Errors", 1)
			n.n.exp.Set("LastError", Time)
			n.n.exp.Set("LastErrorMessage", Err)
			//log.Println(n.n.Name, n.err)
			if n.conn != nil {
				n.conn.Close()
			}
		}
	}
	return n.conn, n.err
}

// NewDriver returns an initialized Cluster driver, using upstreamDriver as backend
func NewDriver(upstreamDriver driver.Driver) Driver {
	m := expvar.NewMap("ClusterSql")
	Time := new(expvar.String)
	Time.Set(time.Now().String())
	m.Set("FirstInstanciated", Time)
	cl := Driver{map[string]*node{}, upstreamDriver, m}
	return cl
}
