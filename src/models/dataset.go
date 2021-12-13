package models

import "sort"

type Dataset struct {
	Schema TableSchema
	Rows []Row
}

func (d *Dataset) getSubColumnDataSet(columnIds []int) Dataset {
	var subColumns []ColumnSchema
	var replyRows []Row
	for _, columnId := range columnIds {
		subColumns = append(subColumns, d.Schema.ColumnSchemas[columnId])
	}
	for i, row := range d.Rows {
		var replyRow Row
		for _, columnId := range columnIds {
			replyRow = append(replyRow, row[columnId])
		}
		replyRow = append(replyRow, i)
		replyRows = append(replyRows, replyRow)
	}
	return Dataset{
		TableSchema{
			d.Schema.TableName,
			subColumns,
		},
		replyRows,
	}
}

func (d *Dataset) getSubRowDataSet(rowIds []int) Dataset {
	var replyRows []Row
	for _, rowId := range rowIds {
		replyRows = append(replyRows, d.Rows[rowId])
	}
	return Dataset{
		d.Schema,
		replyRows,
	}
}

func (d *Dataset) getMergeDataSet(other *Dataset) Dataset {
	resultSchema, okList := d.Schema.getMergeSchema(&other.Schema)
	var resultRows []Row

	id := 0
	allMerged := true
	locA := len(d.Schema.ColumnSchemas)
	locB := len(other.Schema.ColumnSchemas)
	for _, row := range d.Rows {
		if len(row) == len(resultSchema.ColumnSchemas) + 1 {
			resultRows = append(resultRows, row)
			continue
		}

		for id < len(other.Rows) && row[locA].(int) > other.Rows[id][locB].(int) {
			id += 1
		}

		curRow := make(Row, locA)
		copy(curRow, row)
		if id < len(other.Rows) && other.Rows[id][locB].(int) == row[locA].(int) {
			for i, ok := range okList {
				if ok {
					curRow = append(curRow, other.Rows[id][i])
				}
			}
		} else {
			allMerged = false
		}
		curRow = append(curRow, row[locA])
		resultRows = append(resultRows, curRow)
	}

	if allMerged {
		return Dataset{
			resultSchema,
			resultRows,
		}
	} else {
		return Dataset{
			d.Schema,
			resultRows,
		}
	}
}

func (d *Dataset) sortRows() {
	loc := len(d.Schema.ColumnSchemas)
	sort.SliceStable(d.Rows, func(i, j int) bool {
		return d.Rows[i][loc].(int) < d.Rows[j][loc].(int)
	})

	var deduplicatedRows []Row
	for _, row := range d.Rows {
		if len(deduplicatedRows) == 0 || row[loc].(int) > deduplicatedRows[len(deduplicatedRows) - 1][loc].(int) {
			deduplicatedRows = append(deduplicatedRows, row)
		}
	}
	d.Rows = deduplicatedRows
}

func (d *Dataset) changeSchema(schema *TableSchema) {
	ok := true
	var columnIds []int
	for _, columnA := range schema.ColumnSchemas {
		for j, columnB := range d.Schema.ColumnSchemas {
			if columnA == columnB {
				columnIds = append(columnIds, j)
			}
		}
	}
	for i := range columnIds {
		if i != columnIds[i] {
			ok = false
			break
		}
	}

	if !ok {
		for i, row := range d.Rows {
			var newRow Row
			for _, id := range columnIds {
				newRow = append(newRow, row[id])
			}
			newRow = append(newRow, row[len(row) - 1])
			d.Rows[i] = newRow
		}
	}
}

func (d *Dataset) getUnionDataSet(other *Dataset) Dataset {
	resultSchema, okList := d.Schema.getMergeSchema(&other.Schema)
	var resultRows []Row

	loc := len(d.Schema.ColumnSchemas)
	for i := range d.Rows {
		curRow := make(Row, loc)
		copy(curRow, d.Rows[i])
		for j, ok := range okList {
			if ok {
				curRow = append(curRow, other.Rows[i][j])
			}
		}
		resultRows = append(resultRows, curRow)
	}
	return Dataset{
		resultSchema,
		resultRows,
	}
}