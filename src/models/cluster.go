package models

import (
	"../labgob"
	"../labrpc"
	"encoding/json"
	"fmt"
	"strconv"
)

// Cluster consists of a group of nodes to manage distributed tables defined in models/table.go.
// The Cluster object itself can also be viewed as the only coordinator of a cluster, which means client requests
// should go through it instead of the nodes.
// Of course, it is possible to make any of the nodes a coordinator and to make the cluster decentralized. You are
// welcomed to make such changes and may earn some extra points.
type Cluster struct {
	// the identifiers of each node, we use simple numbers like "1,2,3" to register the nodes in the network
	// needless to say, each identifier should be unique
	nodeIds []string
	// the network that the cluster works on. It is not actually using the network interface, but a network simulator
	// using SEDA (google it if you have not heard about it), which allows us (and you) to inject some network failures
	// during tests. Do remember that network failures should always be concerned in a distributed environment.
	network *labrpc.Network
	// the Name of the cluster, also used as a network address of the cluster coordinator in the network above
	Name string

	tableSize map[string]int
	tableSchemaMap map[string]TableSchema
}

// NewCluster creates a Cluster with the given number of nodes and register the nodes to the given network.
// The created cluster will be named with the given one, which will used when a client wants to connect to the cluster
// and send requests to it. WARNING: the given name should not be like "Node0", "Node1", ..., as they will conflict
// with some predefined names.
// The created nodes are identified by simple numbers starting from 0, e.g., if we have 3 nodes, the identifiers of the
// three nodes will be "Node0", "Node1", and "Node2".
// Each node is bound to a server in the network which follows the same naming rule, for the example above, the three
// nodes will be bound to  servers "Node0", "Node1", and "Node2" respectively.
// In practice, we may mix the usages of terms "Node" and "Server", both of them refer to a specific machine, while in
// the lab, a "Node" is responsible for processing distributed affairs but a "Server" simply receives messages from the
// net work.
func NewCluster(nodeNum int, network *labrpc.Network, clusterName string) *Cluster {
	labgob.Register(TableSchema{})
	labgob.Register(Row{})

	nodeIds := make([]string, nodeNum)
	nodeNamePrefix := "Node"
	for i := 0; i < nodeNum; i++ {
		// identify the nodes with "Node0", "Node1", ...
		node := NewNode(nodeNamePrefix + strconv.Itoa(i))
		nodeIds[i] = node.Identifier
		// use go reflection to extract the methods in a Node object and make them as a service.
		// a service can be viewed as a list of methods that a server provides.
		// due to the limitation of the framework, the extracted method must only have two parameters, and the first one
		// is the actual argument list, while the second one is the reference to the result.
		// NOTICE, a REFERENCE should be passed to the method instead of a value
		nodeService := labrpc.MakeService(node)
		// create a server, a server is responsible for receiving requests and dispatching them
		server := labrpc.MakeServer()
		// add the service to the server so the server can provide the services
		server.AddService(nodeService)
		// register the server to the network as "Node0", "Node1", ...
		network.AddServer(nodeIds[i], server)
	}

	// create a cluster with the nodes and the network
	c := &Cluster{
		nodeIds: nodeIds,
		network: network,
		Name: clusterName,
		tableSize: make(map[string]int),
		tableSchemaMap: make(map[string]TableSchema),
	}
	// create a coordinator for the cluster to receive external requests, the steps are similar to those above.
	// notice that we use the reference of the cluster as the name of the coordinator server,
	// and the names can be more than strings.
	clusterService := labrpc.MakeService(c)
	server := labrpc.MakeServer()
	server.AddService(clusterService)
	network.AddServer(clusterName, server)
	return c
}

// SayHello is an example to show how the coordinator communicates with other nodes in the cluster.
// Any method that can be accessed by network clients should have EXACTLY TWO parameters, while the first one is the
// actual parameter desired by the method (can be a list if there are more than one desired parameters), and the second
// one is a reference to the return value. The caller must ensure that the reference is valid (not nil).
func (c *Cluster) SayHello(visitor string, reply *string) {
	endNamePrefix := "InternalClient"
	for _, nodeId := range c.nodeIds {
		// create a client (end) to each node
		// the name of the client should be unique, so we use the name of each node for it
		endName := endNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		// connect the client to the node
		c.network.Connect(endName, nodeId)
		// a client should be enabled before being used
		c.network.Enable(endName, true)
		// call method on that node
		argument := visitor
		reply := ""
		// the first parameter is the name of the method to be called, recall that we use the reference of
		// a Node object to create a service, so the first part of the parameter will be the class name "Node", and as
		// we want to call the method SayHello(), so the second part is "SayHello", and the two parts are separated by
		// a dot
		end.Call("Node.SayHello", argument, &reply)
		fmt.Println(reply)
	}
	*reply = fmt.Sprintf("Hello %s, I am the coordinator of %s", visitor, c.Name)
}

func (c *Cluster) ScanTableWithRowIds(tableSchema *TableSchema, rowIds []int) Dataset {
	var remoteDataSets []Dataset
	endNamePrefix := "InternalClient"
	for _, remoteId := range c.nodeIds {
		remoteEndName := endNamePrefix + remoteId
		remoteEnd := c.network.MakeEnd(remoteEndName)
		c.network.Connect(remoteEndName, remoteId)
		c.network.Enable(remoteEndName, true)

		var remoteDataSet Dataset
		remoteEnd.Call("Node.ScanTableWithRowIds", []interface{}{tableSchema.TableName, rowIds}, &remoteDataSet)
		if len(remoteDataSet.Rows) > 0 {
			remoteDataSets = append(remoteDataSets, remoteDataSet)
		}
	}

	var resultDataSet Dataset
	resultDataSet.Schema = *tableSchema
	for i, remoteDataSet := range remoteDataSets {
		if len(remoteDataSet.Schema.ColumnSchemas) != len(tableSchema.ColumnSchemas) {
			for j := i + 1; j < len(remoteDataSets); j++ {
				remoteDataSet = remoteDataSet.getMergeDataSet(&remoteDataSets[j])
			}
			if len(remoteDataSet.Schema.ColumnSchemas) == len(tableSchema.ColumnSchemas) {
				remoteDataSet.changeSchema(tableSchema)
				remoteDataSets[i] = remoteDataSet
			}
		}
	}

	rowsMap := make(map[int]Row)
	loc := len(tableSchema.ColumnSchemas)
	for _, remoteDataSet := range remoteDataSets {
		if len(remoteDataSet.Schema.ColumnSchemas) == len(tableSchema.ColumnSchemas) {
			for _, row := range remoteDataSet.Rows {
				rowsMap[row[loc].(int)] = row
			}
		}
	}

	var resultRows []Row
	for _, rowId := range rowIds {
		resultRows = append(resultRows, rowsMap[rowId])
	}
	resultDataSet.Rows = resultRows
	return resultDataSet
}

func (c* Cluster) ScanTableWithSchema(tableSchema *TableSchema) Dataset {
	var remoteDataSets []Dataset
	endNamePrefix := "InternalClient"
	for _, remoteId := range c.nodeIds {
		remoteEndName := endNamePrefix + remoteId
		remoteEnd := c.network.MakeEnd(remoteEndName)
		c.network.Connect(remoteEndName, remoteId)
		c.network.Enable(remoteEndName, true)

		var remoteDataSet Dataset
		remoteEnd.Call("Node.ScanTableWithSchema", []interface{}{*tableSchema}, &remoteDataSet)
		if len(remoteDataSet.Rows) > 0 {
			remoteDataSets = append(remoteDataSets, remoteDataSet)
		}
	}

	var resultDataSet Dataset
	resultDataSet.Schema = *tableSchema
	for i, remoteDataSet := range remoteDataSets {
		if len(remoteDataSet.Schema.ColumnSchemas) == len(tableSchema.ColumnSchemas) {
			resultDataSet.Rows = append(resultDataSet.Rows, remoteDataSet.Rows...)
		} else {
			for j := i + 1; j < len(remoteDataSets); j++ {
				remoteDataSet = remoteDataSet.getMergeDataSet(&remoteDataSets[j])
			}
			if len(remoteDataSet.Schema.ColumnSchemas) == len(tableSchema.ColumnSchemas) {
				remoteDataSet.changeSchema(tableSchema)
				resultDataSet.Rows = append(resultDataSet.Rows, remoteDataSet.Rows...)
			}
		}
	}
	resultDataSet.sortRows()
	return resultDataSet
}

// Join all tables in the given list using NATURAL JOIN (join on the common columns), and return the joined result
// as a list of rows and set it to reply.
func (c* Cluster) Join(tableNames []string, reply *Dataset) {
	labgob.Register(Dataset{})
	var cacheDataSet Dataset
	for i := range tableNames {
		if i == len(tableNames) - 1 {
			break
		}

		var remoteDataSet Dataset
		var localDataSet Dataset
		remoteSchema := c.tableSchemaMap[tableNames[i]]
		localSchema := c.tableSchemaMap[tableNames[i + 1]]
		if i == 0 {
			localIds, remoteIds := localSchema.getForeignKeys(remoteSchema)
			if localIds != nil {
				remoteSubSchema := remoteSchema.getSubSchema(remoteIds)
				localSubSchema := localSchema.getSubSchema(localIds)
				remoteDataSet = c.ScanTableWithSchema(&remoteSubSchema)
				localDataSet = c.ScanTableWithSchema(&localSubSchema)
			}
		} else {
			localIds, remoteIds := localSchema.getForeignKeys(cacheDataSet.Schema)
			if localIds != nil {
				remoteDataSet = cacheDataSet.getSubColumnDataSet(remoteIds)
				localSubSchema := localSchema.getSubSchema(localIds)
				localDataSet = c.ScanTableWithSchema(&localSubSchema)
			}
		}

		// semi-join
		var remoteRowIds []int
		var localRowIds []int
		for _, rowA := range remoteDataSet.Rows {
			for _, rowB := range localDataSet.Rows {
				ok := true
				for j := 0; j < len(rowA) - 1; j++ {
					if rowA[j] != rowB[j] {
						ok = false
						break
					}
				}
				if ok {
					remoteRowIds = append(remoteRowIds, rowA[len(rowA) - 1].(int))
					localRowIds = append(localRowIds, rowB[len(rowB) - 1].(int))
				}
			}
		}

		if i == 0 {
			cacheDataSet = c.ScanTableWithRowIds(&remoteSchema, remoteRowIds)
		} else {
			cacheDataSet = cacheDataSet.getSubRowDataSet(remoteRowIds)
		}
		localDataSet = c.ScanTableWithRowIds(&localSchema, localRowIds)

		cacheDataSet = cacheDataSet.getUnionDataSet(&localDataSet)
	}

	*reply = cacheDataSet
}

func (c* Cluster) isNodeExists(nodeId string) bool {
	for _, internalId := range c.nodeIds {
		if nodeId == internalId {
			return true
		}
	}
	return false
}

func (c* Cluster) BuildTable(params []interface{}, reply *string) {
	labgob.Register([]Predicate{})
	schema, schemaErr := params[0].(TableSchema)
	if schemaErr != true {
		*reply = "Build table error: Cannot cast params[0] to type TableSchema!"
		return
	}
	var rules map[string]interface{}
	jsonErr := json.Unmarshal(params[1].([]byte), &rules)
	if jsonErr != nil {
		*reply = "Build table error: Cannot cast params[1] to json!"
		return
	}
	c.tableSize[schema.TableName] = 0
	c.tableSchemaMap[schema.TableName] = schema

	endNamePrefix := "InternalClient"
	nodeNamePrefix := "Node"
	for nodeId, reluMap := range rules {
		if !c.isNodeExists(nodeNamePrefix + nodeId) {
			*reply = "Build table error: Node doesn't exist!"
			return
		}
		rule, ruleErr := reluMap.(map[string]interface{})
		if ruleErr != true {
			*reply = "Build table error: Cannot cast rule in param[1]!"
			return
		}

		endName := endNamePrefix + nodeNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		c.network.Connect(endName, nodeNamePrefix + nodeId)
		c.network.Enable(endName, true)

		var columnSchemas []ColumnSchema
		var columnIds []int
		for _, columnName := range rule["column"].([]interface{}) {
			var dataType = schema.getDataType(columnName.(string))
			var columnId = schema.getColumnId(columnName.(string))
			if dataType == -1 {
				*reply = "Build table error: Unknown ColumnName name!"
				return
			}
			var columnSchema = ColumnSchema{
				columnName.(string),
				dataType,
			}
			columnSchemas = append(columnSchemas, columnSchema)
			columnIds = append(columnIds, columnId)
		}
		tableSchema := TableSchema{
			TableName: schema.TableName,
			ColumnSchemas: columnSchemas,
		}
		var ps []Predicate
		for columnName, predicates := range rule["predicate"].(map[string]interface{}) {
			for _, predicate := range predicates.([]interface{}) {

				var p = Predicate{
					columnName,
					predicate.(map[string]interface{})["op"].(string),
					schema.getDataType(columnName),
					predicate.(map[string]interface{})["val"],
				}
				ps = append(ps, p)
			}
		}

		end.Call("Node.CreateTableRPC", []interface{}{tableSchema, columnIds, ps, schema}, reply)
		if *reply != "" {
			return
		}
 	}

	*reply = "Build table success"
}

func (c* Cluster) FragmentWrite(params []interface{}, reply *string) {
	tableName := params[0].(string)
	row := params[1].(Row)
	rowId := c.tableSize[tableName]
	c.tableSize[tableName] += 1

	endNamePrefix := "InternalClient"
	for _, nodeId := range c.nodeIds {
		endName := endNamePrefix + nodeId
		end := c.network.MakeEnd(endName)
		c.network.Connect(endName, nodeId)
		c.network.Enable(endName, true)

		end.Call("Node.InsertRPC", []interface{}{tableName, row, rowId}, &reply)
		if *reply != "" {
			return
		}
	}

	*reply = "Fragment write success"
}
