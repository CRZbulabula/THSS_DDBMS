package models

import (
	"errors"
	"strconv"
)

type Predicate struct {
	ColumnName string
	Operator string
	DataType int
	Value    interface{}
}

func (p *Predicate) equals(other *Predicate) bool {
	return p.ColumnName == other.ColumnName && p.Operator == other.Operator &&
		p.DataType == other.DataType && p.Value == other.Value
}

func isPredicatesEqual(pa []Predicate, pb []Predicate) bool {
	for _, p1 := range pa {
		existEqual := false
		for _, p2 := range pb {
			if p1.equals(&p2) {
				existEqual = true
				break
			}
		}
		if !existEqual {
			return false
		}
	}
	return true
}

// Predicate getters
func (p *Predicate) getInt32Value() (int32, error) {
	switch p.Value.(type) {
	case int:
		return int32(p.Value.(int)), nil
	case int32:
		return p.Value.(int32), nil
	case int64:
		return int32(p.Value.(int64)), nil
	case float32:
		return int32(p.Value.(float32)), nil
	case float64:
		return int32(p.Value.(float64)), nil
	case bool:
		if p.Value.(bool) {
			return 1, nil
		} else {
			return 0, nil
		}
	case string:
		int32v, err := strconv.Atoi(p.Value.(string))
		return int32(int32v), err
	default:
		return -1, errors.New("unknown data type")
	}
}

func (p *Predicate) getInt64Value() (int64, error) {
	switch p.Value.(type) {
	case int:
		return int64(p.Value.(int)), nil
	case int32:
		return int64(p.Value.(int32)), nil
	case int64:
		return p.Value.(int64), nil
	case float32:
		return int64(p.Value.(float32)), nil
	case float64:
		return int64(p.Value.(float64)), nil
	case bool:
		if p.Value.(bool) {
			return 1, nil
		} else {
			return 0, nil
		}
	case string:
		int64v, err := strconv.ParseInt(p.Value.(string), 10, 64)
		return int64v, err
	default:
		return -1, errors.New("unknown data type")
	}
}

func (p *Predicate) getFloat32Value() (float32, error) {
	switch p.Value.(type) {
	case int:
		return float32(p.Value.(int)), nil
	case int32:
		return float32(p.Value.(int32)), nil
	case int64:
		return float32(p.Value.(int64)), nil
	case float32:
		return p.Value.(float32), nil
	case float64:
		return float32(p.Value.(float64)), nil
	case bool:
		if p.Value.(bool) {
			return 1.0, nil
		} else {
			return 0.0, nil
		}
	case string:
		float32v, err := strconv.ParseFloat(p.Value.(string), 32)
		return float32(float32v), err
	default:
		return -1, errors.New("unknown data type")
	}
}

func (p *Predicate) getFloat64Value() (float64, error) {
	switch p.Value.(type) {
	case int:
		return float64(p.Value.(int)), nil
	case int32:
		return float64(p.Value.(int32)), nil
	case int64:
		return float64(p.Value.(int64)), nil
	case float32:
		return float64(p.Value.(float32)), nil
	case float64:
		return p.Value.(float64), nil
	case bool:
		if p.Value.(bool) {
			return 1.0, nil
		} else {
			return 0.0, nil
		}
	case string:
		float64v, err := strconv.ParseFloat(p.Value.(string), 64)
		return float64v, err
	default:
		return -1, errors.New("unknown data type")
	}
}

func (p *Predicate) getBoolValue() (bool, error) {
	switch p.Value.(type) {
	case int:
		return p.Value.(int) != 0, nil
	case int32:
		return p.Value.(int32) != 0, nil
	case int64:
		return p.Value.(int64) != 0, nil
	case float32:
		return p.Value.(float32) != 0, nil
	case float64:
		return p.Value.(float64) != 0, nil
	case bool:
		return p.Value.(bool), nil
	case string:
		if p.Value.(string) == "true" {
			return true, nil
		} else if p.Value.(string) == "false" {
			return false, nil
		} else {
			return false, errors.New("unknown data type")
		}
	default:
		return false, errors.New("unknown data type")
	}
}

func (p *Predicate) getStringValue() (string, error) {
	switch p.Value.(type) {
	case int:
		return strconv.Itoa(p.Value.(int)), nil
	case int32:
		return strconv.Itoa(int(p.Value.(int32))), nil
	case int64:
		return strconv.FormatInt(p.Value.(int64), 10), nil
	case float32:
		return strconv.FormatFloat(float64(p.Value.(float32)), 'E', -1, 32), nil
	case float64:
		return strconv.FormatFloat(p.Value.(float64), 'E', -1, 32), nil
	case bool:
		if p.Value.(bool) {
			return "true", nil
		} else {
			return "false", nil
		}
	case string:
		return p.Value.(string), nil
	default:
		return "", errors.New("unknown data type")
	}
}