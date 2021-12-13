package models

// ColumnSchema defines the name and the datatype of a column
type ColumnSchema struct {
	Name string
	DataType int // one of datatype.go
}

func (cs *ColumnSchema) equals(other *ColumnSchema) bool {
	return cs.Name == other.Name && cs.DataType == other.DataType
}