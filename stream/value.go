package stream

import (
	"errors"
	"fmt"
)

type Stringify interface {
	//TODO refine me
	AsString() string
}

//Value is interface for Record's V
type Value interface {
	Stringify
	Get(index interface{}) interface{}
	Set(index, value interface{}) error
	GetMany(index ...interface{}) []interface{}
	Copy() Value
}

type TupleValue struct {
	V []interface{}
}

func (tuple *TupleValue) Copy() Value {
	return NewTupleValue(tuple.V...)
}

func NewTupleValue(v ...interface{}) *TupleValue {
	tv := &TupleValue {
		make([]interface{}, 0),
	}
	for _, i := range v {
		tv.V = append(tv.V, i)
	}
	return tv
}

func (tuple *TupleValue) AsString() string {
	return fmt.Sprint(tuple.V)
}

func (tuple *TupleValue) Get(index interface{}) interface{} {
	i, ok := index.(int)
	if !ok {
		return nil
	}
	vl := len(tuple.V)
	if i <= vl {
		return tuple.V[i]
	}
	return nil
}

func (tuple *TupleValue) GetMany(indexes ...interface{}) []interface{} {
	ret := make([]interface{}, len(indexes))
	for i, index := range indexes {
		ret[i] = tuple.Get(index)
	}
	return ret
}

func (tuple *TupleValue) Set(index, value interface{}) error {
	i, ok := index.(int)
	if !ok {
		return errors.New("index should be integer")
	}
	vLen := len(tuple.V)
	if i< 0 || i >= vLen {
		return errors.New("index out of range")
	}
	tuple.V[i] = value
	return nil
}

type MapValue struct {
	V map[string]interface{}
}

func NewMapValue(v map[string]interface{}) *MapValue {
	ret := &MapValue{
		make(map[string]interface{}),
	}
	for k, i := range v {
		ret.V[k] = i
	}
	return ret
}

func (m *MapValue) Copy() *MapValue {
	return NewMapValue(m.V)
}

func (m *MapValue) AsString() string {
	return fmt.Sprint(m.V)
}

func (m *MapValue) Get(index interface{}) interface{} {
	i, ok := index.(string)
	if !ok {
		return nil
	}
	ret, ok := m.V[i]
	if !ok {
		return nil
	}
	return ret
}

func (m *MapValue) Set(index, value interface{}) error {
	i, ok := index.(string)
	if !ok {
		return errors.New("index should be string")
	}
	m.V[i] = value
	return nil
}

func (m *MapValue) GetMany(indexes ...interface{}) []interface{} {
	ret := make([]interface{}, len(indexes))
	for i, index := range indexes {
		ret[i] = m.Get(index)
	}
	return ret
}
