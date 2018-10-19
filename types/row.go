package types

import "github.com/tinylib/msgp/msgp"

//go:generate msgp -o codec_row.go

type ItemType int

const (
	TypeMapRecord ItemType = iota
	TypeTupleRecord
	TypeWatermark
)

// Row is used to transport in Piper
type Row struct {
	item     msgp.Raw
	itemType ItemType
}

func (r Row) Type() ItemType {
	return r.itemType
}

func (r Row) AsMapRecord() (MapRecord, error) {
	mr := MapRecord{}
	_, err := mr.UnmarshalMsg(r.item)
	return mr, err
}

func (r Row) AsTupleRecord() (TupleRecord, error) {
	tr := TupleRecord{}
	_, err := tr.UnmarshalMsg(r.item)
	return tr, err
}

func (r Row) AsWatermark() (Watermark, error) {
	wm := Watermark{}
	_, err := wm.UnmarshalMsg(r.item)
	return wm, err
}
