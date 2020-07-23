package stream

import (
	"github.com/zhnpeng/wstream/intfs"
)

type RescaledStream struct {
	DataStream
	Selector intfs.Selector
}

func NewRescaledStream(flow *Flow, parallel int, selector intfs.Selector) *RescaledStream {
	stm := &RescaledStream{
		DataStream: DataStream{
			flow: flow,
		},
		Selector: selector,
	}
	return stm
}
