package stream

import (
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
)

type WindowedStream struct {
	name     string
	parallel int
	trigger  triggers.Trigger
	operator execution.Operator

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
	return s.operator.New()
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

func (s *WindowedStream) Trigger(trigger triggers.Trigger) *WindowedStream {
	s.trigger = trigger
	return s
}
