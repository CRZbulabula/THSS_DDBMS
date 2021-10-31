package models

import (
	"fmt"
	"testing"
)
import "../labrpc"
import "encoding/json"

func TestLab1Transform(t *testing.T) {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network := labrpc.MakeNetwork()
	c := NewCluster(5, network, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli := network.MakeEnd(clientName)
	network.Connect(clientName, c.Name)
	network.Enable(clientName, true)

	// create fragment rules
	var i interface{}
	err := json.Unmarshal([]byte(`{
		"0": {
			"predicate": {
				"sale_price": [{
					"op": "<",
					"val": 2000
				}], "on_sale" :[{
					"op": "=="
					"val": true
				}]
			} ,"column": [
				"object_id", 
				"object_name", 
				"sale_price",
				"on_sale"
			]
		}, "1": {
			"predicate": {
				"sale_price": [{
					"op": "<",
					"val": 2000
				}], "on_sale" :[{
					"op": "=="
					"val": false
				}]
			} ,"column": [
				"object_id", 
				"object_name", 
				"sale_price",
				"on_sale"
			]
		}, "2": {
			"predicate": {
				"sale_price": [{
					"op": ">",
					"val": 2000
				}, {
					"op": "<=",
					"val": 5000
				}], "on_sale" :[{
					"op": "!="
					"val": false
				}]
			} ,"column": [
				"object_id", 
				"object_name", 
				"sale_price",
				"on_sale"
			]
		}, "3": {
			"predicate": {
				"sale_price": [{
					"op": ">",
					"val": 2000
				}, {
					"op": "<=",
					"val": 5000
				}], "on_sale" :[{
					"op": "!="
					"val": true
				}]
			} ,"column": [
				"object_id", 
				"object_name", 
				"sale_price",
				"on_sale"
			]
		}, "4": {
			"predicate": {
				"sale_price": [{
					"op": ">",
					"val": 5000
				}]
			}, "column" [
				"object_id",
				"object_name",
				"sale_price",
				"on_sale"
			]
		}}`), &i)

	if err != nil {return}
	m := i.(map[string]interface{})
	rules,_ := json.Marshal(m)
	fmt.Printf("map_json=%v\n", string(rules))

	// use the client to create table and insert
	budgetTableName := "sales"
	ts := &TableSchema{TableName: budgetTableName, ColumnSchemas: []ColumnSchema{
		{Name: "object_id", DataType: TypeInt32},
		{Name: "object_name", DataType: TypeString},
		{Name: "sale_price", DataType: TypeDouble},
		{Name: "on_sale", DataType: TypeBoolean},
	}}
	replyMsg := ""
	cli.Call("Cluster.BuildTable", []interface{}{ts, rules}, &replyMsg)

	budgetRows := []Row{
		{1, "toothbrush", 20, "true"},
		{2, "toothpaste", 25, true},
		{3.2, "face wash", 50, "false"},
		{4, "nut", 5, true},
		{"5", "albumen powder", "200", false},
		{6, "laptop", "3000", 1},
		{7, "Just do it", 2500, 0},
		{8, "perfume", 4000, true},
		{9, "diamond", 8888.88, true},
	}
	replyMsg = ""
	for _, row := range budgetRows {
		cli.Call("Cluster.FragmentWrite", []interface{}{budgetTableName, row}, &replyMsg)
	}

	end0 := network.MakeEnd("client0")
	network.Connect("client0", "Node0")
	network.Enable("client0", true)

	table0 := Dataset{}
	end0.Call("Node.ScanTable", budgetTableName, &table0)

	end1 := network.MakeEnd("client1")
	network.Connect("client1", "Node1")
	network.Enable("client1", true)

	table1 := Dataset{}
	end1.Call("Node.ScanTable", budgetTableName, &table1)

	end2 := network.MakeEnd("client2")
	network.Connect("client2", "Node2")
	network.Enable("client2", true)

	table2 := Dataset{}
	end2.Call("Node.ScanTable", budgetTableName, &table2)

	end3 := network.MakeEnd("client3")
	network.Connect("client3", "Node3")
	network.Enable("client3", true)

	table3 := Dataset{}
	end3.Call("Node.ScanTable", budgetTableName, &table3)

	end4 := network.MakeEnd("client4")
	network.Connect("client4", "Node4")
	network.Enable("client4", true)

	table4 := Dataset{}
	end4.Call("Node.ScanTable", budgetTableName, &table4)

	expectedDataset0 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "object_name", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "on_sale", DataType: TypeBoolean},
			},
		},
		Rows:  []Row{
			{1, "toothbrush", 20, true},
			{2, "toothpaste", 25, true},
			{4, "nut", 5, true},
		},
	}

	expectedDataset1 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "object_name", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "on_sale", DataType: TypeBoolean},
			},
		},
		Rows:   []Row{
			{3, "face wash", 50, false},
			{5, "albumen powder", 200, false},
		},
	}

	expectedDataset2 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "object_name", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "on_sale", DataType: TypeBoolean},
			},
		},
		Rows:   []Row{
			{6, "laptop", 3000, true},
			{8, "perfume", 4000, true},
		},
	}

	expectedDataset3 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "object_name", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "on_sale", DataType: TypeBoolean},
			},
		},
		Rows:   []Row{
			{7, "Just do it", 2500, false},
		},
	}

	expectedDataset4 := Dataset{
		Schema: TableSchema{
			"",
			[]ColumnSchema{
				{Name: "object_id", DataType: TypeInt32},
				{Name: "object_name", DataType: TypeString},
				{Name: "sale_price", DataType: TypeDouble},
				{Name: "on_sale", DataType: TypeBoolean},
			},
		},
		Rows:   []Row{
			{9, "diamond", 8888.88, true},
		},
	}

	if !compareDataset(expectedDataset0, table0) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset0, table0)
	}
	if !compareDataset(expectedDataset1, table1) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset1, table1)
	}
	if !compareDataset(expectedDataset2, table2) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset2, table2)
	}
	if !compareDataset(expectedDataset3, table3) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset3, table3)
	}
	if !compareDataset(expectedDataset4, table4) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset4, table4)
	}
}