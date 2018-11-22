package stream

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/operator"
	"github.com/wandouz/wstream/runtime/selector"
)

type RescaledStream struct {
	DataStream
	selector intfs.Selector
}

func NewRescaledStream(flow *Flow, parallel int, selector *selector.Selector) *RescaledStream {
	stm := &RescaledStream{
		DataStream: DataStream{
			flow:     flow,
			operator: operator.NewByPass(),
		},
		selector: selector,
	}
	return stm
}
