package items

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
	if err != nil {
		return mr, err
	}
	return mr, nil
}

func (r Row) AsTupleRecord() (TupleRecord, error) {
	tr := TupleRecord{}
	_, err := tr.UnmarshalMsg(r.item)
	if err != nil {
		return tr, err
	}
	return tr, nil
}

func (r Row) AsWatermark() (Watermark, error) {
	wm := Watermark{}
	_, err := wm.UnmarshalMsg(r.item)
	if err != nil {
		return wm, err
	}
	return wm, nil
}
