package models

type Predicate struct {
	ColumnName string
	Operator string
	DataType int
	Value    interface{}
}