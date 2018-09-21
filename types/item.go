package types

import "time"

type Item interface {
	Type() ItemType
	AsRow() (Row, error)
	Time() time.Time
	SetTime(t time.Time)
}
