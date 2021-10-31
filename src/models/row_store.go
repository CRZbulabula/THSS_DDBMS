package models

import (
	"container/list"
	"errors"
	"strconv"
)

// Row is just an array of objects
type Row []interface{}

// Row getters
func (r *Row) getInt32Value(columnId int) (int32, error) {
	switch (*r)[columnId].(type) {
		case int:
			return int32((*r)[columnId].(int)), nil
		case int32:
			return (*r)[columnId].(int32), nil
		case int64:
			return int32((*r)[columnId].(int64)), nil
		case float32:
			return int32((*r)[columnId].(float32)), nil
		case float64:
			return int32((*r)[columnId].(float64)), nil
		case bool:
			if (*r)[columnId].(bool) {
				return 1, nil
			} else {
				return 0, nil
			}
		case string:
			int32v, err := strconv.Atoi((*r)[columnId].(string))
			return int32(int32v), err
		default:
			return -1, errors.New("unknown data type")
	}
}

func (r *Row) getInt64Value(columnId int) (int64, error) {
	switch (*r)[columnId].(type) {
		case int:
			return int64((*r)[columnId].(int)), nil
		case int32:
			return int64((*r)[columnId].(int32)), nil
		case int64:
			return (*r)[columnId].(int64), nil
		case float32:
			return int64((*r)[columnId].(float32)), nil
		case float64:
			return int64((*r)[columnId].(float64)), nil
		case bool:
			if (*r)[columnId].(bool) {
				return 1, nil
			} else {
				return 0, nil
			}
		case string:
			int64v, err := strconv.ParseInt((*r)[columnId].(string), 10, 64)
			return int64v, err
		default:
			return -1, errors.New("unknown data type")
	}
}

func (r *Row) getFloat32Value(columnId int) (float32, error) {
	switch (*r)[columnId].(type) {
		case int:
			return float32((*r)[columnId].(int)), nil
		case int32:
			return float32((*r)[columnId].(int32)), nil
		case int64:
			return float32((*r)[columnId].(int64)), nil
		case float32:
			return (*r)[columnId].(float32), nil
		case float64:
			return float32((*r)[columnId].(float64)), nil
		case bool:
			if (*r)[columnId].(bool) {
				return 1.0, nil
			} else {
				return 0.0, nil
			}
		case string:
			float32v, err := strconv.ParseFloat((*r)[columnId].(string), 32)
			return float32(float32v), err
		default:
			return -1, errors.New("unknown data type")
	}
}

func (r *Row) getFloat64Value(columnId int) (float64, error) {
	switch (*r)[columnId].(type) {
		case int:
			return float64((*r)[columnId].(int)), nil
		case int32:
			return float64((*r)[columnId].(int32)), nil
		case int64:
			return float64((*r)[columnId].(int64)), nil
		case float32:
			return float64((*r)[columnId].(float32)), nil
		case float64:
			return (*r)[columnId].(float64), nil
		case bool:
			if (*r)[columnId].(bool) {
				return 1.0, nil
			} else {
				return 0.0, nil
			}
		case string:
			float64v, err := strconv.ParseFloat((*r)[columnId].(string), 64)
			return float64v, err
		default:
			return -1, errors.New("unknown data type")
	}
}

func (r *Row) getBoolValue(columnId int) (bool, error) {
	switch (*r)[columnId].(type) {
		case int:
			return (*r)[columnId].(int) != 0, nil
		case int32:
			return (*r)[columnId].(int32) != 0, nil
		case int64:
			return (*r)[columnId].(int64) != 0, nil
		case float32:
			return (*r)[columnId].(float32) != 0, nil
		case float64:
			return (*r)[columnId].(float64) != 0, nil
		case bool:
			return (*r)[columnId].(bool), nil
		case string:
			if (*r)[columnId].(string) == "true" {
				return true, nil
			} else if (*r)[columnId].(string) == "false" {
				return false, nil
			} else {
				return false, errors.New("unknown data type")
			}
		default:
			return false, errors.New("unknown data type")
	}
}

func (r *Row) getStringValue(columnId int) (string, error) {
	switch (*r)[columnId].(type) {
		case int:
			return strconv.Itoa((*r)[columnId].(int)), nil
		case int32:
			return strconv.Itoa(int((*r)[columnId].(int32))), nil
		case int64:
			return strconv.FormatInt((*r)[columnId].(int64), 10), nil
		case float32:
			return strconv.FormatFloat(float64((*r)[columnId].(float32)), 'E', -1, 32), nil
		case float64:
			return strconv.FormatFloat((*r)[columnId].(float64), 'E', -1, 32), nil
		case bool:
			if (*r)[columnId].(bool) {
				return "true", nil
			} else {
				return "false", nil
			}
		case string:
			return (*r)[columnId].(string), nil
		default:
			return "", errors.New("unknown data type")
	}
}

// Equals compares two rows by their length and each element
func (r *Row) Equals(another *Row) bool {
	if len(*r) != len(*another) {
		return false
	}
	for i, val := range *r {
		if val != (*another)[i] {
			return false
		}
	}
	return true
}

// EqualsWithColumnMapping compares two rows each element with the provided columnMapping, which indicate the index of
// each ColumnName of this row in another row. This method assumes, as the columnMapping is provided, the two rows have the
// same length.
func (r *Row) EqualsWithColumnMapping(another *Row, columnMapping []int) bool {
	for i, column := range *r {
		if column != (*another)[columnMapping[i]] {
			return false
		}
	}
	return true
}

// RowStore manages the storage of rows and provide simple read-write interfaces.
// Notice that the store does not guarantee any constraints, and it is the responsibility of the caller to check
// constraints like primary key and uniqueness before calling the methods in RowStore.
type RowStore interface {
	count() int
	iterator() RowIterator
	// the row will be copied into the store instead of directly store the reference
	insert(row *Row)
	// only removes the first row that equals to the argument
	remove(row *Row)
}

// RowIterator iterates rows in a RowStore.
type RowIterator interface {
	HasNext() bool
	Next() *Row
}

// MemoryListRowStore uses a linked list to store rows in memory.
type MemoryListRowStore struct {
	rows *list.List
}

func NewMemoryListRowStore() *MemoryListRowStore {
	return &MemoryListRowStore{rows: list.New()}
}

func (s *MemoryListRowStore) count() int {
	return s.rows.Len()
}

func (s *MemoryListRowStore) iterator() RowIterator {
	return NewMemoryListRowIterator(s.rows)
}

func (s *MemoryListRowStore) insert(row *Row) {
	s.rows.PushBack(*row)
}

func (s *MemoryListRowStore) remove(row *Row) {
	curr := s.rows.Front()
	for curr != nil {
		// find the first row that equals the argument
		r,_ := curr.Value.(Row)
		if r.Equals(row) {
			s.rows.Remove(curr)
			return
		}
		curr = curr.Next()
	}
}

type MemoryListRowIterator struct {
	next *list.Element
	rows *list.List
}

func NewMemoryListRowIterator(rows *list.List) RowIterator{
	iter := &MemoryListRowIterator{rows.Front(), rows}
	return iter
}

func (iter *MemoryListRowIterator) HasNext() bool {
	return iter.next != nil
}

func (iter *MemoryListRowIterator) Next() *Row {
	if iter.next == nil {
		return nil
	} else {
		t,_ := iter.next.Value.(Row)
		iter.next = iter.next.Next()
		return &t
	}
}





