package stream


type ItemType int

const (
	TypeRecord = iota
	TypeWatermark
)

type Item interface {
	Type() ItemType
	AsRecord() *Record
	AsWatermark() *Watermark
}

type BasicItem struct {
}

func (i *BasicItem) AsRecord() *Record {
	return nil
}

func (i *BasicItem) AsWatermark() *Watermark {
	return nil
}

type ItemChan chan Item