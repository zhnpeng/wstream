package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/selector"
)

type DataStream struct {
	parallel int
	operator intfs.Operator

	// flow reference
	streamNode *StreamNode
	flow       *Flow
}

/*
DataStream API
*/

func NewDataStream(flow *Flow, parallel int) *DataStream {
	return &DataStream{
		parallel: parallel,
		flow:     flow,
	}
}

func (s *DataStream) Operator() intfs.Operator {
	return s.operator.New()
}

func (s *DataStream) Parallelism() int {
	return s.parallel
}

func (s *DataStream) clone() *DataStream {
	return &DataStream{
		flow:     s.flow,
		parallel: s.parallel,
	}
}

func (s *DataStream) SetStreamNode(node *StreamNode) {
	s.streamNode = node
}

func (s *DataStream) GetStreamNode() (node *StreamNode) {
	return s.streamNode
}

func (s *DataStream) toKeyedStream(keys []interface{}) *KeyedStream {
	return NewKeyedStream(s.flow, s.parallel, keys)
}

func (s *DataStream) toRescaleStream(parallel int, selector *selector.Selector) *RescaledStream {
	return NewRescaledStream(s.flow, parallel, selector)
}

func (s *DataStream) connect(stream Stream) error {
	return s.flow.AddStreamEdge(s, stream)
}

func (s *DataStream) Debug(debugFunc functions.Debug) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewDebug(debugFunc)
	s.connect(stream)
	return stream
}
