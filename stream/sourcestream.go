package stream

import (
	"encoding/gob"

	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/types"
)

func init() {
	gob.Register(&SourceStream{})
}

// SourceStream accept channels as inputs
type SourceStream struct {
	DataStream
	inputs []chan types.Item
}

func NewSourceStream(flow *Flow) *SourceStream {
	stm := &SourceStream{
		DataStream: DataStream{
			flow:     flow,
			operator: operator.NewByPass(),
		},
	}
	flow.AddStream(stm)
	return stm
}

func (s *SourceStream) Inputs() []chan types.Item {
	return s.inputs
}

func (s *SourceStream) Channels(inputs ...chan types.Item) *SourceStream {
	for _, input := range inputs {
		s.inputs = append(s.inputs, input)
		s.DataStream.parallel++
	}
	return s
}

func (s *SourceStream) MapChannels(inputs ...chan map[string]interface{}) *SourceStream {
	for _, input := range inputs {
		ain := make(chan types.Item)
		s.inputs = append(s.inputs, ain)
		s.DataStream.parallel++
		go func(in chan map[string]interface{}) {
			for {
				m, ok := <-in
				if !ok {
					close(ain)
					break
				}
				ain <- types.NewRawMapRecord(m)
			}
		}(input)
	}
	return s
}
