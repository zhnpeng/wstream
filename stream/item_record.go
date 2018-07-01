package stream

import (
	"time"
)

type Record struct {
	BasicItem
	V Value
	T time.Time
}

func NewRecord(t time.Time, v Value) Item {
	return &Record{
		T: t,
		V: v,
	}
}

//Copy create a new record of this record pass it to new stream
func (e *Record) Copy(newValue Value) *Record {
	return &Record{
		V: newValue,
		T: e.T,
	}
}

func (e *Record) Type() ItemType {
	return TypeRecord
}

func (e *Record) AsRecord() *Record {
	return e
}

func (e *Record) Time() time.Time {
	return e.T
}

func (e *Record) GetValue() Value {
	return e.V
}

func (e *Record) SetValue(v Value) {
	e.V = v
}
