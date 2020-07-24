package stream

import (
	"encoding/gob"

	"github.com/zhnpeng/wstream/runtime/execution"
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
			flow: flow,
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
		s.DataStream.Parallel++
	}
	return s
}

func (s *SourceStream) toTask() *execution.Task {
	// TODO: find a better implement to add source input channel to task's nodes
	task := s.DataStream.toTask()
	for i, node := range task.BroadcastNodes {
		node.AddInEdge(s.inputs[i])
	}
	return task
}

func (s *SourceStream) MapChannels(inputs ...chan map[string]interface{}) *SourceStream {
	for _, input := range inputs {
		ain := make(chan types.Item)
		s.inputs = append(s.inputs, ain)
		s.DataStream.Parallel++
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
