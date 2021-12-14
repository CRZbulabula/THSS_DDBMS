package models

import (
	"../labrpc"
	"encoding/json"
	"testing"
)

const stTableName = "st"
const tsTableName = "ts"

var stTableSchema *TableSchema
var tsTableSchema *TableSchema

var stRows []Row
var tsRows []Row

var stJoinedTableSchema TableSchema

var stJoinedTableContent []Row

var stTablePartitionRules []byte
var tsTablePartitionRules []byte

func stDefineTables() {
	stTableSchema = &TableSchema{
		TableName: stTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "sid", DataType: TypeInt32},
			{Name: "tid", DataType: TypeInt32},
			{Name: "sname", DataType: TypeString},
		},
	}

	tsTableSchema = &TableSchema{
		TableName: tsTableName,
		ColumnSchemas: []ColumnSchema{
			{Name: "sid", DataType: TypeInt32},
			{Name: "tid", DataType: TypeInt32},
			{Name: "tname", DataType: TypeString},
		},
	}

	stRows = []Row{
		{0, 0, "John"},
		{0, 1, "Smith"},
		{1, 0, "Hana"},
		{1, 1, "Emma"},
	}

	tsRows = []Row{
		{0, 0, "Olivia"},
		{0, 1, "Ava"},
		{1, 0, "Isabella"},
		{1, 1, "Sophia"},
	}

	stJoinedTableSchema = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{Name: "tid", DataType: TypeInt32},
			{Name: "sname", DataType: TypeString},
			{Name: "tname", DataType: TypeString},
		},
	}

	stJoinedTableContent = []Row{
		{0, 0, "John", "Olivia"},
		{0, 1, "Smith", "Ava"},
		{1, 0, "Hana", "Isabella"},
		{1, 1, "Emma", "Sophia"},
	}
}

func stSetupCli() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network = labrpc.MakeNetwork()
	c = NewCluster(3, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli = network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)
}

func TestLab2MultiForeignKeyTableJoin(t *testing.T) {
	stSetupCli()
	stDefineTables()

	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"sid",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tid",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"sname",
			},
		},
	}
	stTablePartitionRules, _ = json.Marshal(m)

	m = map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tname",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tid",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"sid",
			},
		},
	}
	tsTablePartitionRules, _ = json.Marshal(m)

	stBuildTables(cli)
	stInsertData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{stTableName, tsTableName}, &results)
	expectedDataset := Dataset{
		Schema: stJoinedTableSchema,
		Rows: stJoinedTableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func TestLab3MultiForeignKeyTableJoin(t *testing.T) {
	stSetupCli()
	stDefineTables()

	m := map[string]interface{}{
		"0|1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"sid",
			},
		},
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tid", "sname",
			},
		},
	}
	stTablePartitionRules, _ = json.Marshal(m)

	m = map[string]interface{}{
		"1|2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"tname", "tid",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"sid": [...]map[string]interface{}{{
					"op": ">=",
					"val": 0,
				}},
			},
			"column": [...]string{
				"sid",
			},
		},
	}
	tsTablePartitionRules, _ = json.Marshal(m)

	stBuildTables(cli)
	stInsertData(cli)

	// perform a join and check the result
	results := Dataset{}
	cli.Call("Cluster.Join", []string{stTableName, tsTableName}, &results)
	expectedDataset := Dataset{
		Schema: stJoinedTableSchema,
		Rows: stJoinedTableContent,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func stBuildTables(cli *labrpc.ClientEnd)  {
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{stTableSchema, stTablePartitionRules}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{tsTableSchema, tsTablePartitionRules}, &replyMsg)
}

func stInsertData(cli *labrpc.ClientEnd) {
	replyMsg := ""
	for _, row := range stRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{stTableName, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range tsRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{tsTableName, row}, &replyMsg)
	}
}