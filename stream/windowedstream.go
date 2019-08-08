package stream

import (
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
)

type WindowedStream struct {
	parallel   int
	assigner   assigners.WindowAssinger
	trigger    triggers.Trigger
	evictor    evictors.Evictor
	operator   intfs.Operator
	flow       *Flow
	streamNode *StreamNode
}

func NewWindowedStream(flow *Flow, parallel int) *WindowedStream {
	return &WindowedStream{
		flow:     flow,
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

func (s *WindowedStream) Operator() intfs.Operator {
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

func (s *WindowedStream) toDataStream() *DataStream {
	return NewDataStream(s.flow, s.parallel)
}

// left merge new stream to windowed stream and then return new stream
func (s *WindowedStream) leftMerge(stream Stream) {
	s.flow.LeftMergeStream(s, stream)
}
