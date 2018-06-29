package stream

import (
	"time"
	"errors"
)

type Map struct {
	K []interface{}
	V map[string]interface{}
	T time.Time
}

func (e *Map) Type() EventType {
	return MapEvent
}

func (e *Map) Time() time.Time {
	return e.T
}

func (e *Map) Keys() []interface{} {
	return e.K
}

//Get get value by index, index must to be int in Map event
//index is begein with 0
func (e *Map) Get(index interface{}) interface{} {
	i, ok := index.(string)
	if !ok {
		return nil
	}
	if v, ok := e.V[i]; ok {
		return v
	}
	return nil
}

func (e *Map) GetMany(indexes ...interface{}) []interface{} {
	many := make([]interface{}, len(indexes))
	for i, index := range indexes {
		many[i] = e.Get(index)
	}
	return many
}

func (e *Map) Set(index, value interface{}) error {
	i, ok := index.(string)
	if !ok {
		return errors.New("type assertion error")
	}
	e.V[i] = value
	return nil
}

func (e *Map) UseKeys(indexes ...interface{}) error {
	var keys []interface{}
	vLen := len(e.V)
	for _, index := range indexes {
		i, ok := index.(string)
		if !ok {
			return errors.New("type aeertion error")
		}
		if ret, ok := e.V[i]; ok {
			keys = append(keys, ret)
		} else {
			keys = append(keys, nil)
		}
	}
	e.K = keys
	return nil
}