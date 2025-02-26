package models

import (
	"errors"
	"fmt"
	"strconv"
)

// Node manages some tables defined in models/table.go
type Node struct {
	// the name of the Node, and it should be unique across the cluster
	Identifier string
	// tableName -> table
	TableMap map[string]*Table
	// tableName -> original table schema
	SchemaMap map[string]TableSchema
	// tableName -> cluster table column ids
	columnIdsMap map[string][]int
	// tableName -> ColumnName predicate
	predicates map[string][]Predicate
}

// NewNode creates a new node with the given name and an empty set of tables
func NewNode(id string) *Node {
	return &Node{
		Identifier: id,
		TableMap: make(map[string]*Table),
		SchemaMap: make(map[string]TableSchema),
		columnIdsMap: make(map[string][]int),
		predicates: make(map[string][]Predicate),
	}
}

// SayHello is an example about how to create a method that can be accessed by RPC (remote procedure call, methods that
// can be called through network from another node). RPC methods should have exactly two arguments, the first one is the
// actual argument (or an argument list), while the second one is a reference to the result.
func (n *Node) SayHello(args interface{}, reply *string) {
	// NOTICE: use reply (the second parameter) to pass the return value instead of "return" statements.
	*reply = fmt.Sprintf("Hello %s, I am Node %s", args, n.Identifier)
}

// CreateTableRPC is an RPC interface for create table on specified node
func (n *Node) CreateTableRPC(args []interface{}, reply *string) {
	schema, schemaErr := args[0].(TableSchema)
	columnIds, columnIdsErr := args[1].([]int)
	predicates, predicatesErr := args[2].([]Predicate)
	origin, originErr := args[3].(TableSchema)
	if schemaErr != true {
		*reply = "Create table error: Cannot cast params[0] to type TableSchema!"
		return
	}
	if columnIdsErr != true {
		*reply = "Create table error: Cannot cast params[1] to type []int!"
		return
	}
	if predicatesErr != true {
		*reply = "Create table error: Cannot cast params[1] to type TableSchema!"
	}
	if originErr != true {
		*reply = "Create table error: Cannot cast params[1] to type []Predicate!"
	}

	// check if table partition already exists
	tableExists := false
	for tableCount := 0; ; tableCount++ {
		if table, ok := n.TableMap[schema.TableName + "-" + strconv.Itoa(tableCount)]; ok {
			schemaEqual := table.schema.equals(&schema)
			predicateEqual := isPredicatesEqual(n.predicates[table.schema.TableName], predicates)
			if schemaEqual && predicateEqual {
				tableExists = true
				break
			} else if !schemaEqual && predicateEqual {
				// merge schema
				mergedSchema, okList := table.schema.getMergeSchema(&schema)
				mergedSchema.TableName = schema.TableName + "-" + strconv.Itoa(tableCount)
				table.schema = &mergedSchema
				for i, ok2 := range okList {
					if ok2 {
						n.columnIdsMap[mergedSchema.TableName] = append(n.columnIdsMap[mergedSchema.TableName], columnIds[i])
					}
				}

				tableExists = true
				break
			}
		} else {
			schema.TableName = schema.TableName + "-" + strconv.Itoa(tableCount)
			break
		}
	}
	if tableExists {
		return
	}

	err := n.CreateTable(&schema)
	if err != nil {
		*reply = err.Error()
		return
	}
	n.columnIdsMap[schema.TableName] = columnIds
	for _, predicate := range predicates {
		n.predicates[schema.TableName] = append(n.predicates[schema.TableName], predicate)
	}
	n.SchemaMap[schema.TableName] = origin
}

// CreateTable creates a Table on this node with the provided schema. It returns nil if the table is created
// successfully, or an error if another table with the same name already exists.
func (n *Node) CreateTable(schema *TableSchema) error {
	// check if the table already exists
	if _, ok := n.TableMap[schema.TableName]; ok {
		return errors.New("table already exists")
	}
	// create a table and store it in the map
	t := NewTable(
		schema,
		NewMemoryListRowStore(),
	)
	n.TableMap[schema.TableName] = t
	return nil
}

// InsertRPC is an RPC interface for insert a row into specified table
func (n *Node) InsertRPC(args []interface{}, reply *string) {
	tableName := args[0].(string)
	row := args[1].(Row)
	rowId := args[2].(int)

	for tableCount := 0; ; tableCount++ {
		pTableName := tableName + "-" + strconv.Itoa(tableCount)
		if n.predicates[pTableName] == nil {
			return
		}

		ok, err := n.PredicateCheck(pTableName, &row)
		if err != nil {
			*reply = err.Error()
			return
		}
		if ok {
			var insertRow Row
			for _, columnId := range n.columnIdsMap[pTableName] {
				insertRow = append(insertRow, row[columnId])
			}
			insertRow = append(insertRow, rowId)
			err = n.Insert(pTableName, &insertRow)
			if err != nil {
				*reply = err.Error()
			}
		}
	}

}

// PredicateCheck checks whether a row is satisfied all predicates
func (n *Node) PredicateCheck(tableName string, row *Row) (bool, error) {
	ps := n.predicates[tableName]
	schema := n.SchemaMap[tableName]
	for _, p := range ps {
		var lessFlag, equalFlag bool
		columnId := schema.getColumnId(p.ColumnName)
		// get comparison results based on data types
		switch p.DataType {
			case TypeInt32:
				rowValue, err := row.getInt32Value(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getInt32Value()
				if err != nil {
					return false, err
				}
				lessFlag = rowValue < pValue
				equalFlag = rowValue == pValue
				break
			case TypeInt64:
				rowValue, err := row.getInt64Value(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getInt64Value()
				if err != nil {
					return false, err
				}
				lessFlag = rowValue < pValue
				equalFlag = rowValue == pValue
				break
			case TypeFloat:
				rowValue, err := row.getFloat32Value(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getFloat32Value()
				if err != nil {
					return false, err
				}
				lessFlag = rowValue < pValue
				equalFlag = rowValue == pValue
				break
			case TypeDouble:
				rowValue, err := row.getFloat64Value(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getFloat64Value()
				if err != nil {
					return false, err
				}
				lessFlag = rowValue < pValue
				equalFlag = rowValue == pValue
				break
			case TypeBoolean:
				rowValue, err := row.getBoolValue(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getBoolValue()
				if err != nil {
					return false, err
				}
				lessFlag = false
				equalFlag = rowValue == pValue
				break
			case TypeString:
				rowValue, err := row.getStringValue(columnId)
				if err != nil {
					return false, err
				}
				pValue, err := p.getStringValue()
				if err != nil {
					return false, err
				}
				lessFlag = rowValue < pValue
				equalFlag = rowValue == pValue
				break
		}
		// check predicates
		switch p.Operator {
			case "<":
				if !lessFlag {
					return false, nil
				}
				break
			case "<=":
				if !lessFlag && !equalFlag {
					return false, nil
				}
				break
			case "==":
				if !(equalFlag) {
					return false, nil
				}
				break
			case ">":
				if lessFlag || equalFlag {
					return false, nil
				}
			case ">=":
				if lessFlag {
					return false, nil
				}
				break
			case "!=":
				if equalFlag {
					return false, nil
				}
				break
		}
	}

	return true, nil
}

// Insert inserts a row into the specified table, and returns nil if succeeds or an error if the table does not exist.
func (n *Node) Insert(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Insert(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// Remove removes a row from the specified table, and returns nil if succeeds or an error if the table does not exist.
// It does not concern whether the provided row exists in the table.
func (n *Node) Remove(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Remove(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// IterateTable returns an iterator of the table through which the caller can retrieve all rows in the table in the
// order they are inserted. It returns (iterator, nil) if the Table can be found, or (nil, err) if the Table does not
// exist.
func (n *Node) IterateTable(tableName string) (RowIterator, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.RowIterator(), nil
	} else {
		return nil, errors.New("no such table")
	}
}

// IterateTable returns the count of rows in a table. It returns (cnt, nil) if the Table can be found, or (-1, err)
// if the Table does not exist.
func (n *Node) count(tableName string) (int, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.Count(), nil
	} else {
		return -1, errors.New("no such table")
	}
}

func (n *Node) ScanTableWithRowIds(args []interface{}, datasets *[]Dataset) {
	tableName := args[0].(string)
	rowIds := args[1].([]int)

	for tableCount := 0; ; tableCount++ {
		pTableName := tableName + "-" + strconv.Itoa(tableCount)
		if t, ok := n.TableMap[pTableName]; ok {
			iterator := t.RowIterator()
			loc := len(t.schema.ColumnSchemas)
			rowsMap := make(map[int]Row)

			for iterator.HasNext() {
				curRow := *iterator.Next()
				rowsMap[curRow[loc].(int)] = curRow
			}

			var resultRows []Row
			for _, id := range rowIds {
				if rowsMap[id] != nil {
					resultRows = append(resultRows, rowsMap[id])
				}
			}
			*datasets = append(*datasets, Dataset{
				*t.schema,
				resultRows,
			})
		} else {
			break
		}
	}

}

func (n *Node) ScanTableWithSchema(args []interface{}, datasets *[]Dataset) {
	tableSchema := args[0].(TableSchema)
	for tableCount := 0; ; tableCount++ {
		pTableName := tableSchema.TableName + "-" + strconv.Itoa(tableCount)
		if t, ok := n.TableMap[pTableName]; ok {
			var resultColumns []ColumnSchema
			var resultIds []int
			for _, columnA := range tableSchema.ColumnSchemas {
				for i, columnB := range t.schema.ColumnSchemas {
					if columnA == columnB {
						resultColumns = append(resultColumns, columnA)
						resultIds = append(resultIds, i)
					}
				}
			}
			if len(resultIds) == 0 {
				return
			}
			resultIds = append(resultIds, len(t.schema.ColumnSchemas))

			var resultRows []Row
			iterator := t.RowIterator()
			for iterator.HasNext() {
				curRow := *iterator.Next()
				var row Row
				for _, id := range resultIds {
					row = append(row, curRow[id])
				}

				resultRows = append(resultRows, row)
			}

			*datasets = append(*datasets, Dataset{
				TableSchema{
					t.schema.TableName,
					resultColumns,
				},
				resultRows,
			})
		} else {
			break
		}
	}
}

// ScanTable returns all rows in a table by the specified name or nothing if it does not exist.
// This method is recommended only to be used for TEST PURPOSE, and try not to use this method in your implementation,
// but you can use it in your own test cases.
// The reason why we deprecate this method is that in practice, every table is so large that you cannot transfer a whole
// table through network all at once, so sending a whole table in one RPC is very impractical. One recommended way is to
// fetch a batch of Rows a time.
func (n *Node) ScanTable(tableName string, dataset *Dataset) {
	for tableCount := 0; ; tableCount++ {
		pTableName := tableName + "-" + strconv.Itoa(tableCount)
		if t, ok := n.TableMap[pTableName]; ok {
			resultSet := Dataset{}

			tableRows := make([]Row, t.Count())
			i := 0
			iterator := t.RowIterator()
			for iterator.HasNext() {
				curRow := *iterator.Next()
				tableRows[i] = curRow[ : len(curRow) - 1]
				i = i + 1
			}

			resultSet.Rows = tableRows
			resultSet.Schema = *t.schema
			*dataset = resultSet
		} else {
			break
		}
	}
}