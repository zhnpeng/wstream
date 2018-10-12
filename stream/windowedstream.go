package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator"
	"github.com/wandouz/wstream/runtime/operator/windowing/assigners"
	"github.com/wandouz/wstream/runtime/operator/windowing/evictors"
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
)

type WindowedStream struct {
	name     string
	parallel int
	assigner assigners.WindowAssinger
	trigger  triggers.Trigger
	evictor  evictors.Evictor

	applyFunc  functions.ApplyFunc
	reduceFunc functions.ReduceFunc
	operator   execution.Operator

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

func (s *WindowedStream) Trigger(trigger triggers.Trigger) *WindowedStream {
	s.trigger = trigger
	s.operator = operator.NewWindow(s.assigner, s.trigger)
	return s
}

func (s *WindowedStream) Evict(evictor evictors.Evictor) *WindowedStream {
	s.evictor = evictor
	if evictor != nil {
		s.operator = operator.NewEvictWindow(s.assigner, s.trigger, evictor)
	}
	return s
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
