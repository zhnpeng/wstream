package types

//go:generate msgp -o codec_tuple_item.go

import (
	"errors"
	"fmt"
	"time"
)

type TupleRecord struct {
	T time.Time
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

func (tuple *TupleRecord) AsRow() (Row, error) {
	encodedBytes, err := tuple.UnmarshalMsg(nil)
	if err != nil {
		return Row{}, err
	}
	return Row{
		itemType: TypeTupleRecord,
		item:     encodedBytes,
	}, nil
}

func (tuple *TupleRecord) Copy() Record {
	return NewTupleRecord(tuple.T, tuple.V...)
}

func (tuple *TupleRecord) AsString() string {
	return fmt.Sprint(tuple.V)
}

func (tuple *TupleRecord) Get(index interface{}) interface{} {
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
	vLen := len(tuple.V)
	if i < 0 || i >= vLen {
		return errors.New("index out of range")
	}
	tuple.V[i] = value
	return nil
}