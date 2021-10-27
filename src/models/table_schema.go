package models

// TableSchema contains the name of the table and the definition of each column
type TableSchema struct {
	TableName string
	ColumnSchemas []ColumnSchema
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