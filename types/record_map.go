package types

import (
	"errors"
	"time"
)

//go:generate msgp -o codec_map_item.go

type MapRecord struct {
	T time.Time
	K []interface{}
	V map[string]interface{}
}

func NewMapRecord(t time.Time, v map[string]interface{}) *MapRecord {
	return &MapRecord{
		T: t,
		V: v,
	}
}

func (m *MapRecord) Time() time.Time {
	return m.T
}

func (m *MapRecord) SetTime(t time.Time) {
	m.T = t
}

func (m *MapRecord) Type() ItemType {
	return TypeMapRecord
}

func (m *MapRecord) AsRow() (Row, error) {
	encodedBytes, err := m.MarshalMsg(nil)
	if err != nil {
		return Row{}, err
	}
	return Row{
		itemType: TypeMapRecord,
		item:     encodedBytes,
	}, nil
}

func (m *MapRecord) Copy() Record {
	return NewMapRecord(m.T, m.V)
}

func (m *MapRecord) Get(index interface{}) interface{} {
	i, ok := index.(string)
	if !ok {
		return nil
	}
	if v, ok := m.V[i]; ok {
		return v
	}
	return nil
}

func (m *MapRecord) GetMany(indexes ...interface{}) []interface{} {
	ret := make([]interface{}, len(indexes))
	for i, index := range indexes {
		ret[i] = m.Get(index)
	}
	return ret
}

func (m *MapRecord) Set(index, value interface{}) error {
	i, ok := index.(string)
	if !ok {
		return errors.New("index should be string")
	}
	m.V[i] = value
	return nil
}

func (m *MapRecord) UseKeys(indexes ...interface{}) []interface{} {
	keys := m.GetMany(indexes)
	m.K = keys
	return keys
}
