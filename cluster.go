package clustersql

import "database/sql/driver"
import "log"
import "time"

type ClusterError struct {
	Message string
}

func (me ClusterError) Error() string {
	return me.Message
}

type Cluster struct {
	Nodes  map[string]*Node
	Driver driver.Driver
}

func (cluster *Cluster) AddNode(nodeName, dataSourceName string) {
	cluster.Nodes[nodeName] = &Node{nodeName, dataSourceName, nil, false, nil}
}

type Node struct {
	Name           string
	dataSourceName string
	Conn           driver.Conn
	Waiting        bool
	Err            error
}

func (cluster *Cluster) getConn() (driver.Conn, error) {
	nodec := make(chan *Node)
	die := make(chan bool)
	for _, node := range cluster.Nodes {
		if !node.Waiting {
			node.Waiting = true
			go func(nodec chan *Node, node Node, die chan bool) {
				cluster.Nodes[node.Name] = &node
				node.Waiting = true
				node.Conn, node.Err = cluster.Driver.Open(node.dataSourceName)
				node.Waiting = false
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
	case <-time.After(30 * time.Second):
		return nil, ClusterError{"Could not open any connection!"}
	}
}

func (cluster Cluster) Prepare(query string) (driver.Stmt, error) {
	conn, err := cluster.getConn()
	if err != nil {
		return nil, err
	}
	//log.Println(query)
	return conn.Prepare(query)
}

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

func (cluster Cluster) Begin() (driver.Tx, error) {
	conn, err := cluster.getConn()
	if err != nil {
		return nil, err
	}
	return conn.Begin()
}

type ClusterDriver struct {
	cluster *Cluster
}

func NewDriver(upstreamDriver driver.Driver) ClusterDriver {
	return ClusterDriver{&Cluster{map[string]*Node{}, upstreamDriver}}
}

func (d *ClusterDriver) AddNode(name, dataSourceName string) {
	d.cluster.AddNode(name, dataSourceName)
}

func (d ClusterDriver) Open(name string) (driver.Conn, error) {
	return d.cluster, nil
}
