package stream

import (
	"errors"
	"time"
)

type Tuple struct {
	K []interface{}
	V []interface{}
	T time.Time
}

func (e *Tuple) Type() EventType {
	return TupleEvent
}

func (e *Tuple) Time() time.Time {
	return e.T
}

func (e *Tuple) Keys() []interface{} {
	return e.K
}

//Get get value by index, index must to be int in tuple event
//index is begein with 0
func (e *Tuple) Get(index interface{}) interface{} {
	i, ok := index.(int)
	if !ok {
		return nil
	}
	vl := len(e.V)
	if i <= vl {
		return e.V[i]
	}
	return nil
}

func (e *Tuple) GetMany(indexes ...interface{}) (many []interface{}) {
	many = make([]interface{}, len(indexes))
	for i, index := range indexes {
		many[i] = e.Get(index)
	}
	return many
}

//Set will raise error if index >= length, and will append if index < 0
func (e *Tuple) Set(index, value interface{}) error {
	i, ok := index.(int)
	if !ok {
		return errors.New("type assertion error")
	}
	if i < 0 {
		// append if index < 0
		e.V = append(e.V, value)
		return nil
	}
	if i >= len(e.V) {
		return errors.New("index out of range")
	}
	e.V[i] = value
	return nil
}

func (e *Tuple) UseKeys(indexes ...interface{}) error {
	var keys []interface{}
	vLen := len(e.V)
	for _, index := range indexes {
		i, ok := index.(int)
		if !ok {
			return errors.New("type aeertion error")
		}
		if i < vLen {
			keys = append(keys, e.V[i])
		} else {
			keys = append(keys, nil)
		}
	}
	e.K = keys
	return nil
}
