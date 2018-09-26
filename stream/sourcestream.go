package stream

import (
	"github.com/wandouz/wstream/runtime/operator"
	"github.com/wandouz/wstream/types"
)

// SourceStream accept channels as inputs
type SourceStream struct {
	DataStream
	parallel int
	Inputs   []chan types.Item
}

func NewSourceStream(name string, graph *StreamGraph) *SourceStream {
	stm := &SourceStream{
		DataStream: DataStream{
			name:     name,
			graph:    graph,
			operator: operator.GenRescaleRoundRobin(),
		},
	}
	graph.AddStream(stm)
	return stm
}

func (s *SourceStream) Channels(inputs ...chan types.Item) *SourceStream {
	s.Inputs = inputs
	return s
}
