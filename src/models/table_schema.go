package models

// TableSchema contains the name of the table and the definition of each column
type TableSchema struct {
	TableName string
	ColumnSchemas []ColumnSchema
}

func (ts* TableSchema) equals(other *TableSchema) bool {
	for _, c1 := range ts.ColumnSchemas {
		match := false
		for _, c2 := range other.ColumnSchemas {
			if c1.equals(&c2) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
}

func (ts* TableSchema) getColumnId(columnName string) int {
	for columnId, columnSchema := range ts.ColumnSchemas {
		if columnName == columnSchema.Name {
			return columnId
		}
	}
	return -1
}

func (ts* TableSchema) getDataType(columnName string) int {
	for _, columnSchema := range ts.ColumnSchemas {
		if columnName == columnSchema.Name {
			return columnSchema.DataType
		}
	}
	return -1
}

func (ts* TableSchema) getForeignKeys(joinSchema TableSchema) ([]int, []int) {
	var localIds []int
	var remoteIds []int
	for i, columnI := range ts.ColumnSchemas {
		for j, columnJ := range joinSchema.ColumnSchemas {
			if columnI.Name == columnJ.Name {
				localIds = append(localIds, i)
				remoteIds = append(remoteIds, j)
			}
		}
	}
	if len(localIds) > 0 {
		return localIds, remoteIds
	} else {
		return nil, nil
	}
}

func (ts* TableSchema) getSubSchema(columnIds []int) TableSchema {
	var columns []ColumnSchema
	for _, columnId := range columnIds {
		columns = append(columns, ts.ColumnSchemas[columnId])
	}
	return TableSchema{
		ts.TableName,
		columns,
	}
}

func (ts* TableSchema) getMergeSchema(other *TableSchema) (TableSchema, []bool) {
	mergeColumns := make([]ColumnSchema, len(ts.ColumnSchemas))
	copy(mergeColumns, ts.ColumnSchemas)
	okList := make([]bool, len(other.ColumnSchemas))
	for i, columnA := range other.ColumnSchemas {
		okList[i] = true
		for _, columnB := range ts.ColumnSchemas {
			if columnA == columnB {
				okList[i] = false
				break
			}
		}
		if okList[i] {
			mergeColumns = append(mergeColumns, columnA)
		}
	}
	return TableSchema{
		ts.TableName,
		mergeColumns,
	}, okList
}