package types

//go:generate msgp -o codec_watermark_item.go

import (
	"time"
)

// Watermark is kind of item
type Watermark struct {
	T time.Time
}

func NewWatermark(t time.Time) *Watermark {
	return &Watermark{T: t}
}

func (wm *Watermark) Type() ItemType {
	return TypeWatermark
}

func (wm *Watermark) Time() time.Time {
	return wm.T
}

func (wm *Watermark) AsRow() (Row, error) {
	encodedBytes, err := wm.MarshalMsg(nil)
	if err != nil {
		return Row{}, err
	}
	return Row{
		itemType: TypeWatermark,
		item:     encodedBytes,
	}, nil
}
