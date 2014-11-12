# clustersql [![Build Status](https://travis-ci.org/benthor/clustersql.png)](https://travis-ci.org/benthor/clustersql) [![GoDoc](https://godoc.org/github.com/benthor/clustersql?status.svg)](http://godoc.org/github.com/benthor/clustersql)


Go Clustering SQL Driver - A clustering, implementation-agnostic "meta"-driver for any backend implementing "database/sql/driver".

It does (latency-based) load-balancing and error-recovery over all registered nodes.

**It is assumed that database-state is transparently replicated over all nodes by some database-side clustering solution. This driver ONLY handles the client side of such a cluster.**

This package simply multiplexes the driver.Open() function of sql/driver to every attached node. The function is called on each node, returning the first successfully opened connection. (Any connections opening subsequently will be closed.) If opening does not succeed for any node, the latest error gets returned. Any other errors will be masked by default. However, any given latest error for any attached node will remain exposed through expvar, as well as some basic counters and timestamps.
    
To make use of this kind of clustering, use this package with any backend driver implementing "database/sql/driver" like so:

	import "database/sql"
	import "github.com/go-sql-driver/mysql"
	import "github.com/benthor/clustersql"

There is currently no way around instanciating the backend driver explicitly

	mysqlDriver := mysql.MySQLDriver{}

You can perform backend-driver specific settings such as

	err := mysql.SetLogger(mylogger)

Create a new clustering driver with the backend driver

	clusterDriver := clustersql.NewDriver(mysqlDriver)

Add nodes, including driver-specific name format, in this case Go-MySQL DSN. Here, we add three nodes belonging to a [galera](https://mariadb.com/kb/en/mariadb/documentation/replication-cluster-multi-master/galera/) cluster

	clusterDriver.AddNode("galera1", "user:password@tcp(dbhost1:3306)/db")
	clusterDriver.AddNode("galera2", "user:password@tcp(dbhost2:3306)/db")
	clusterDriver.AddNode("galera3", "user:password@tcp(dbhost3:3306)/db")

Make the clusterDriver available to the go sql interface under an arbitrary name

	sql.Register("myCluster", clusterDriver)

Open the registered clusterDriver with an arbitrary DSN string (not used)

	db, err := sql.Open("myCluster", "whatever")

Continue to use the sql interface as documented at http://golang.org/pkg/database/sql/


Before using this in production, you should configure your cluster details in config.toml and run

    go test -v .

Note however, that non-failure of the above is no guarantee for a correctly set-up cluster.
