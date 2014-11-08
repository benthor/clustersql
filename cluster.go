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

import "database/sql/driver"
import "log"
import "time"

// ClusterError is an error type which represents an unrecoverable Error
type ClusterError struct {
	Message string
}

func (ce ClusterError) Error() string {
	return ce.Message
}

// Cluster is a type which implements "database/sql/driver"
type Cluster struct {
	Nodes  []*Node       // registered node instances
	Driver driver.Driver // the upstream database driver
}

// AddNode registeres backend connection information with the driver
//
// dataSourceName will get passed to the "Open" call of the backend driver
func (cluster *Cluster) AddNode(nodeName, dataSourceName string) {
	cluster.Nodes = append(cluster.Nodes, &Node{nodeName, dataSourceName, nil, false, nil})
}

// Node is a type describing one node in the Cluster
type Node struct {
	Name           string      // node name
	dataSourceName string      // DSN for the backend driver
	Conn           driver.Conn // A currently cached backend connection
	Waiting        bool        // the node is currently waiting for the backend to open a connection
	Err            error       // the last error that was seen by the node
}

type conn struct {
	conn driver.Conn
	err  error
}

// GetConn concurrently opens connections to all nodes in the cluster, returning the first successfully opened driver.Conn.
//
// When opening of a connection fails, an automatic retry after 30 seconds (currently hardcoded) is scheduled. This also generates a log entry with error details.
//
// If no connections could be opened within 45 seconds (currently hardcoded), a ClusterError is returned.
func (cluster *Cluster) GetConnOld() (driver.Conn, error) {
	nodec := make(chan *Node)
	die := make(chan bool)
	for _, node := range cluster.Nodes {
		if !node.Waiting {
			node.Waiting = true
			go func(nodec chan *Node, node Node, die chan bool) {
				//cluster.Nodes[node.Name] = &node
				node.Waiting = true
				for {
					node.Conn, node.Err = cluster.Driver.Open(node.dataSourceName)
					if node.Err != nil {
						log.Println(node.Name, node.Err)
						<-time.Tick(30 * time.Second)
					} else {
						node.Waiting = false
						break
					}
				}
				select {
				case nodec <- &node:
					// log.Println(node.Name, "connected")
				case <-die:
					// log.Println(node.Name, "dying")
					if node.Conn != nil {
						node.Conn.Close()
					} else {
						log.Println(node.Name, node.Err)
					}
				}
			}(nodec, *node, die)
		} else {
			// log.Println(node.Name, "waiting")
		}
	}
	select {
	case node := <-nodec:
		close(die)
		return node.Conn, node.Err
	case <-time.After(45 * time.Second):
		//leave select
	}
	// go 1.0 expects return at the end of a function outside of any block
	return nil, ClusterError{"Could not open any connection!"}
}

func (cluster *Cluster) GetConn() (driver.Conn, error) {
	die := make(chan bool)
	connc := make(chan conn)
	for _, node := range cluster.Nodes {
		go func(node *Node, connc chan conn, die chan bool) {
			c, err := cluster.Driver.Open(node.dataSourceName)
			select {
			case connc <- conn{c, err}:
				log.Println("selected", node.Name)
			case <-die:
				if c != nil {
					c.Close()
				}
			}
		}(node, connc, die)
	}
	var c conn
	for c = range connc {
		if c.conn != nil {
			close(die)
			break
		} else {
			log.Println(c.err)
		}
	}
	return c.conn, c.err
}

// Prepare works as documented at http://golang.org/pkg/database/sql/#DB.Prepare
//
// The query is executed on the node that reponds quickest
func (cluster Cluster) Prepare(query string) (driver.Stmt, error) {
	conn, err := cluster.GetConn()
	if err != nil {
		return nil, err
	}
	//log.Println(query)
	return conn.Prepare(query)
}

// Close works on all backend-connections that are the clusterDriver has cached
//
// Always returns nil for now, errors are merely logged
func (cluster Cluster) Close() error {
	for name, node := range cluster.Nodes {
		if node.Conn != nil {
			if err := node.Conn.Close(); err != nil {
				log.Println(name, err)
			}
		}
	}
	//FIXME
	return nil
}

// Begin works as documented at http://golang.org/pkg/database/sql/#DB.Begin
//
// Begin() is called on the backend connection that is available quickest
func (cluster Cluster) Begin() (driver.Tx, error) {
	conn, err := cluster.GetConn()
	if err != nil {
		return nil, err
	}
	return conn.Begin()
}

// Open is a stub implementation to satisfy database/sql/driver interface. It does not do anything apart from returning its parent type.
// NOTE: While the name argument does not do anything at this point, this may change in the future to allow the setting of e.g., timeout options
func (cluster Cluster) Open(name string) (driver.Conn, error) {
	return cluster, nil
}

// NewDriver returns an initialized Cluster driver, using upstreamDriver as backend
func NewDriver(upstreamDriver driver.Driver) Cluster {
	return Cluster{[]*Node{}, upstreamDriver}
}
