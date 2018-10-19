package stream

import (
	"github.com/wandouz/wstream/runtime/operator"
	"github.com/wandouz/wstream/types"
)

// SourceStream accept channels as inputs
type SourceStream struct {
	DataStream
	Inputs []chan types.Item
}

func NewSourceStream(flow *Flow) *SourceStream {
	stm := &SourceStream{
		DataStream: DataStream{
			flow:     flow,
			operator: operator.NewRescaleRoundRobin(),
		},
	}
	flow.AddStream(stm)
	return stm
}

func (s *SourceStream) Channels(inputs ...chan types.Item) *SourceStream {
	s.Inputs = inputs
	return s
}
