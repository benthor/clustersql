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

//import "time"

// ClusterError is an error type which represents an unrecoverable Error
type ClusterError struct {
	Message string
}

func (ce ClusterError) Error() string {
	return ce.Message
}

// Cluster is a type which implements "database/sql/driver"
type Cluster struct {
	Nodes   []*Node       // registered node instances
	Driver  driver.Driver // the upstream database driver
	getconn chan *Conn
	reqconn chan bool
}

// AddNode registeres backend connection information with the driver
//
// dataSourceName will get passed to the "Open" call of the backend driver
func (cluster *Cluster) AddNode(nodeName, dataSourceName string) {
	_, err := cluster.Driver.Open(dataSourceName)
	if err != nil {
		log.Printf("Not adding Node '%s': %s\n", nodeName, err)
	} else {
		cluster.Nodes = append(cluster.Nodes, &Node{nodeName, dataSourceName}) //, nil, false, nil})
	}
}

// Node is a type describing one node in the Cluster
type Node struct {
	Name           string // node name
	dataSourceName string // DSN for the backend driver
}

type Conn struct {
	Conn driver.Conn
	Err  error
}

func (cluster *Cluster) getConn() {
	connc := make(chan *Conn)
	for {
		die := make(chan bool)
		//log.Println("waiting for request")
		_, ok := <-cluster.reqconn
		if !ok {
			return
		}
		//log.Println("got request")
		for _, node := range cluster.Nodes {
			// if node.Waiting == true {
			// 	continue
			// }
			go func(node *Node, connc chan *Conn, die chan bool) {
				//log.Println("opening node")
				conn, err := cluster.Driver.Open(node.dataSourceName)

				//node.Conn, node.Err = cluster.Driver.Open(node.dataSourceName)
				// if node.Err != nil {
				// 	//log.Println("caught:", node.Err)
				// }
				//log.Println("opened node")
				select {
				case connc <- &Conn{conn, err}:
				case <-die:
					if conn != nil {
						conn.Close()
					}
				}
			}(node, connc, die)

		}
		//log.Println("getting node")
		var c *Conn
		// for c = range connc {
		// 	if c.Err == nil {
		// 		break
		// 	}
		// 	//log.Println(c.Err)
		// }
		c = <-connc
		//n.Waiting = true
		//log.Println("got node, waiting to send connection")
		cluster.getconn <- c
		//log.Println("sent connection")
		close(die)
	}
}

// GetConn concurrently opens connections to all nodes in the cluster, returning the first successfully opened driver.Conn.
// If no driver.Conn could be successfully opened, return the latest error
func (cluster *Cluster) GetConn() *Conn {
	cluster.reqconn <- true
	n := <-cluster.getconn
	// die := make(chan bool)
	// nodec := make(chan *Node)
	// for _, node := range cluster.Nodes {
	// 	go func(node *Node, nodec chan *Node, die chan bool) {
	// 		node.Conn, node.Err = cluster.Driver.Open(node.dataSourceName)
	// 		select {
	// 		case nodec <- node:
	// 			//log.Println("selected", node.Name)
	// 		case <-die:
	// 			//TODO: find out if this is redundant
	// 			// if node.Conn != nil {
	// 			// 	node.Conn.Close()
	// 			// }
	// 		}
	// 	}(node, nodec, die)
	// }
	// var n *Node
	// // for n = range nodec {
	// // 	if n.Conn != nil {
	// // 		close(die)
	// // 		break
	// // 	} else {
	// // 		//log.Println(n.Err)
	// // 	}
	// // }
	// n = <-nodec
	// close(die)
	return n
}

// Prepare works as documented at http://golang.org/pkg/database/sql/#DB.Prepare
//
// The query is executed on the node that reponds quickest
func (cluster Cluster) Prepare(query string) (driver.Stmt, error) {
	n := cluster.GetConn()
	if n.Err != nil {
		return nil, n.Err
	}
	//log.Println(query)
	stmt, err := n.Conn.Prepare(query)
	return stmt, err
}

// Close works on all backend-connections that are the clusterDriver has cached
//
// Always returns nil for now, errors are merely logged
func (cluster Cluster) Close() error {
	var err error
	// for name, node := range cluster.Nodes {
	// if node.Conn != nil {
	// 	if err = node.Conn.Close(); err != nil {
	// 		log.Println(name, err)
	// 	}
	// }
	// }
	cluster.getconn = nil
	close(cluster.reqconn)
	return err
}

// Begin works as documented at http://golang.org/pkg/database/sql/#DB.Begin
//
// Begin() is called on the backend connection that is available quickest
func (cluster Cluster) Begin() (driver.Tx, error) {
	n := cluster.GetConn()
	if n.Err != nil {
		return nil, n.Err
	}
	tx, err := n.Conn.Begin()
	//n.Waiting = false
	return tx, err
}

// Open is a stub implementation to satisfy database/sql/driver interface. It returns an error if the cluster has already been opened (and hasn't been closed)
//
// NOTE: While the name argument does not do anything at this point, this may change in the future to allow the setting of e.g., timeout options
func (cluster Cluster) Open(name string) (driver.Conn, error) {
	if cluster.getconn != nil || cluster.reqconn != nil {
		return nil, ClusterError{"Cluster already opened and wasn't closed"}
	}
	cluster.getconn = make(chan *Conn)
	cluster.reqconn = make(chan bool)
	go cluster.getConn()
	return cluster, nil
}

// NewDriver returns an initialized Cluster driver, using upstreamDriver as backend
func NewDriver(upstreamDriver driver.Driver) Cluster {
	cl := Cluster{[]*Node{}, upstreamDriver, nil, nil}
	return cl
}
