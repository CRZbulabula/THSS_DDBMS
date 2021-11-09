package models

type Dataset struct {
	Schema TableSchema
	Rows []Row
}

func (d *Dataset) getColumn(columnName string) Dataset {
	var replyRows []Row
	columnId := d.Schema.getColumnId(columnName)
	for _, row := range d.Rows {
		replyRow := Row([]interface{}{row[columnId]})
		replyRows = append(replyRows, replyRow)
	}
	return Dataset{
		TableSchema{
			d.Schema.TableName + columnName,
			[]ColumnSchema {d.Schema.ColumnSchemas[columnId]},
		},
		replyRows,
	}
}