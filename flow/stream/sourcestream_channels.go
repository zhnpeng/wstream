package stream

import "github.com/wandouz/wstream/types"

// SourceStreamChannels accept channels as inputs
type SourceStreamChannels struct {
	DataStream
	Inputs []chan types.Item
}

func NewSourceStream(name string, graph *StreamGraph, options map[string]interface{}) *SourceStreamChannels {
	return &SourceStreamChannels{
		DataStream: DataStream{
			name:    name,
			graph:   graph,
			options: options,
		},
	}
}

func (s *SourceStreamChannels) Type() StreamType {
	return TypeSourceStream
}

func (s *SourceStreamChannels) Channels(inputs ...chan types.Item) *SourceStreamChannels {
	s.Inputs = inputs
	return s
}
