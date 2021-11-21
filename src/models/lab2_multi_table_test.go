package models

import (
	"../labrpc"
	"encoding/json"
	"testing"
)

const courseTableName = "course"
const teacherTableName = "teacher"

var courseTableSchema *TableSchema
var teacherTableSchema *TableSchema

var courseRows []Row
var teacherRows []Row

var joinedTableSchemaA TableSchema
var joinedTableSchemaB TableSchema

var joinedTableContentA []Row
var joinedTableContentB []Row

var courseTablePartitionRules []byte
var teacherTablePartitionRules []byte

func MDefineTables() {
	studentTableSchema = &TableSchema{
		TableName: studentTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "sid", DataType: TypeInt32},
			{Name: "sname", DataType: TypeString},
			{Name: "age", DataType: TypeInt32},
			{Name: "grade", DataType: TypeFloat},
		},
	}

	courseRegistrationTableSchema = &TableSchema{
		TableName: courseRegistrationTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "sid", DataType: TypeInt32},
			{Name: "courseId", DataType: TypeInt32},
		},
	}

	courseTableSchema = &TableSchema{
		TableName: courseTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "courseId", DataType: TypeInt32},
			{Name: "cname", DataType: TypeString},
		},
	}

	teacherTableSchema = &TableSchema{
		TableName: teacherTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "tid", DataType: TypeInt32},
			{Name: "courseId", DataType: TypeInt32},
			{Name: "tname", DataType: TypeString},
		},
	}

	studentRows = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
		{3, "Emma", 20, 4.0},
		{4, "Olivia", 21, 3.6},
		{5, "Ava", 22, 4.0},
		{6, "Isabella", 21, 4.0},
		{7, "Sophia", 22, 3.6},
		{8, "Mia", 21, 4.0},
		{9, "Charlotte", 21, 4.0},
	}

	courseRegistrationRows = []Row{
		{0, 0},
		{2, 0},
		{4, 0},
		{6, 0},
		{8, 0},
		{1, 1},
		{3, 1},
		{5, 1},
		{7, 1},
		{9, 1},
		{0, 2},
		{1, 2},
		{2, 2},
		{3, 2},
		{4, 2},
	}

	courseRows = []Row{
		{0, "Linear Algebra"},
		{1, "Go Programming"},
		{2, "Sociology"},
	}

	teacherRows = []Row{
		{0, 2, "Amelia"},
		{1, 1, "Evelyn"},
		{2, 0, "Abigail"},
	}

	joinedTableSchemaA = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"sname", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
			{"cname", TypeString},
		},
	}

	joinedTableContentA = []Row{
		{0, "John", 22, 4.0, 0, "Linear Algebra"},
		{0, "John", 22, 4.0, 2, "Sociology"},
		{1, "Smith", 23, 3.6, 1, "Go Programming"},
		{1, "Smith", 23, 3.6, 2, "Sociology"},
		{2, "Hana", 21, 4.0, 0, "Linear Algebra"},
		{2, "Hana", 21, 4.0, 2, "Sociology"},
		{3, "Emma", 20, 4.0, 1, "Go Programming"},
		{3, "Emma", 20, 4.0, 2, "Sociology"},
		{4, "Olivia", 21, 3.6, 0, "Linear Algebra"},
		{4, "Olivia", 21, 3.6, 2, "Sociology"},
		{5, "Ava", 22, 4.0, 1, "Go Programming"},
		{6, "Isabella", 21, 4.0, 0, "Linear Algebra"},
		{7, "Sophia", 22, 3.6, 1, "Go Programming"},
		{8, "Mia", 21, 4.0, 0, "Linear Algebra"},
		{9, "Charlotte", 21, 4.0, 1, "Go Programming"},
	}

	joinedTableSchemaB = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"sname", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
			{"cname", TypeString},
			{"tid", TypeInt32},
			{"tname", TypeString},
		},
	}

	joinedTableContentB = []Row{
		{0, "John", 22, 4.0, 0, "Linear Algebra", 2, "Abigail"},
		{0, "John", 22, 4.0, 2, "Sociology", 0, "Amelia"},
		{1, "Smith", 23, 3.6, 1, "Go Programming", 1, "Evelyn"},
		{1, "Smith", 23, 3.6, 2, "Sociology", 0, "Amelia"},
		{2, "Hana", 21, 4.0, 0, "Linear Algebra", 2, "Abigail"},
		{2, "Hana", 21, 4.0, 2, "Sociology", 0, "Amelia"},
		{3, "Emma", 20, 4.0, 1, "Go Programming", 1, "Evelyn"},
		{3, "Emma", 20, 4.0, 2, "Sociology", 0, "Amelia"},
		{4, "Olivia", 21, 3.6, 0, "Linear Algebra", 2, "Abigail"},
		{4, "Olivia", 21, 3.6, 2, "Sociology", 0, "Amelia"},
		{5, "Ava", 22, 4.0, 1, "Go Programming", 1, "Evelyn"},
		{6, "Isabella", 21, 4.0, 0, "Linear Algebra", 2, "Abigail"},
		{7, "Sophia", 22, 3.6, 1, "Go Programming", 1, "Evelyn"},
		{8, "Mia", 21, 4.0, 0, "Linear Algebra", 2, "Abigail"},
		{9, "Charlotte", 21, 4.0, 1, "Go Programming", 1, "Evelyn"},
	}
}

func setupCli() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = NewCluster(4, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)
}

func TestLab2MultiTableJoin(t *testing.T) {
	setupCli()
	MDefineTables()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": "<=",
					"val": 6,
				}},
			},
			"column": [...]string{
				"sid", "sname",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": "<=",
					"val": 6,
				}},
				"grade": [...]map[string]interface{}{{
					"op":  "<",
					"val": 4.0,
				}},
			},
			"column": [...]string{
				"age", "grade",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": "<=",
					"val": 6,
				}},
				"grade": [...]map[string]interface{}{{
					"op":  "==",
					"val": 4.0,
				}},
			},
			"column": [...]string{
				"age", "grade",
			},
		},
		"3": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">",
					"val": 6,
				}},
			},
			"column": [...]string{
				"sid", "sname", "age", "grade",
			},
		},
	}
	studentTablePartitionRules, _ = json.Marshal(m)

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">",
					"val": 4,
				}},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4,
				}},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules, _ = json.Marshal(m)

	// assign teacher to node0
	m = map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"tid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tid", "courseId", "tname",
			},
		},
	}
	teacherTablePartitionRules, _ = json.Marshal(m)

	// assign course to node 2
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"courseId", "cname",
			},
		},
	}
	courseTablePartitionRules, _ = json.Marshal(m)

	MBuildTables(cli)
	MInsertData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName, courseTableName}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchemaA,
		Rows: joinedTableContentA,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}

	results = Dataset{}
	cli.Call("Cluster.Join", []string{studentTableName, courseRegistrationTableName, courseTableName, teacherTableName}, &results)
	expectedDataset = Dataset{
		Schema: joinedTableSchemaB,
		Rows: joinedTableContentB,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func MBuildTables(cli *labrpc.ClientEnd)  {
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{courseRegistrationTableSchema, courseRegistrationTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{studentTableSchema, studentTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{teacherTableSchema, teacherTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{courseTableSchema, courseTablePartitionRules}, &replyMsg)
}

func MInsertData(cli *labrpc.ClientEnd) {
	replyMsg := ""
	for _, row := range studentRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range courseRegistrationRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range courseRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range teacherRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{teacherTableName, row}, &replyMsg)
	}
}