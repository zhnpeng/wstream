package stream

import (
	"time"
)

type Watermark struct {
	BasicItem
	T time.Time
}

func (e *Watermark) Type() ItemType {
	return TypeWatermark
}

func (e *Watermark) AsWatermark() *Watermark {
	return e
}