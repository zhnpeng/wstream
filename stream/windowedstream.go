package stream

import (
	"github.com/wandouz/wstream/runtime/execution"
)

type WindowedStream struct {
	name     string
	parallel int
	operator func() execution.Operator

	graph      *StreamGraph
	streamNode *StreamNode
}

func NewWindowedStream(name string, graph *StreamGraph, parallel int) *WindowedStream {
	return &WindowedStream{
		name:     name,
		graph:    graph,
		parallel: parallel,
	}
}

func (s *WindowedStream) Operator() execution.Operator {
	return s.operator()
}

func (s *WindowedStream) Parallelism() int {
	return s.parallel
}

func (s *WindowedStream) SetStreamNode(node *StreamNode) {
	s.streamNode = node
}

func (s *WindowedStream) GetStreamNode() (node *StreamNode) {
	return s.streamNode
}

func (s *WindowedStream) ToDataStream(name string) *DataStream {
	return NewDataStream(
		name,
		s.graph,
		s.parallel,
	)
}
