package stream

import "github.com/wandouz/wstream/types"

type SourceStream struct {
	Basic
	Inputs []types.ItemChan
}

func NewSourceStream(name string, graph *StreamGraph, options map[string]interface{}) *SourceStream {
	return &SourceStream{
		Basic: Basic{
			name:    name,
			graph:   graph,
			options: options,
		},
	}
}

func (s *SourceStream) Type() StreamType {
	return TypeSourceStream
}

func (s *SourceStream) Channels(inputs ...types.ItemChan) *SourceStream {
	s.Inputs = inputs
	return s
}
