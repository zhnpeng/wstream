package types

//go:generate msgp -o codec_tuple_item.go

import (
	"errors"
	"time"
)

type TupleRecord struct {
	T time.Time
	K []interface{}
	V []interface{}
}

func NewTupleRecord(t time.Time, v ...interface{}) *TupleRecord {
	tv := &TupleRecord{
		T: t,
		V: make([]interface{}, 0),
	}
	tv.V = append(tv.V, v...)
	return tv
}

func (tuple *TupleRecord) Type() ItemType {
	return TypeTupleRecord
}

func (tuple *TupleRecord) Time() time.Time {
	return tuple.T
}

func (tuple *TupleRecord) SetTime(t time.Time) {
	tuple.T = t
}

func (tuple *TupleRecord) AsRow() Row {
	encodedBytes, err := tuple.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	return Row{
		itemType: TypeTupleRecord,
		item:     encodedBytes,
	}
}

func (tuple *TupleRecord) Clone() Item {
	tmp := make([]interface{}, len(tuple.V))
	copy(tuple.V, tmp)
	return &TupleRecord{
		T: tuple.T,
		K: tuple.K,
		V: tmp,
	}
}

func (tuple *TupleRecord) Get(index interface{}) interface{} {
	i, ok := index.(int)
	if !ok {
		return nil
	}
	if i < 0 || i >= len(tuple.V) {
		return nil
	}
	return tuple.V[i]
}

func (tuple *TupleRecord) GetMany(indexes ...interface{}) []interface{} {
	ret := make([]interface{}, len(indexes))
	for i, index := range indexes {
		ret[i] = tuple.Get(index)
	}
	return ret
}

func (tuple *TupleRecord) Set(index, value interface{}) error {
	i, ok := index.(int)
	if !ok {
		return errors.New("index should be integer")
	}
	if i < 0 { // if i < 0 append to tail
		tuple.V = append(tuple.V, value)
		return nil
	}
	vLen := len(tuple.V)
	if i >= vLen {
		return errors.New("index out of range")
	}
	tuple.V[i] = value
	return nil
}

func (tuple *TupleRecord) Inherit(record Record) Record {
	tuple.T = record.Time()
	tuple.K = record.Key()
	return tuple
}

// UseKeys use indexes key's values as record's key
func (tuple *TupleRecord) UseKeys(indexes ...interface{}) []interface{} {
	keys := tuple.GetMany(indexes...)
	tuple.K = keys
	return keys
}

func (tuple *TupleRecord) Key() []interface{} {
	return tuple.K
}

func (tuple *TupleRecord) SetKey(k []interface{}) {
	tuple.K = k
}
