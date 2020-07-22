package stream

import (
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/selector"
)

type KeyedStream struct {
	Parallel  int
	Selector_ intfs.Selector
	Operator_ intfs.Operator

	graph      *Flow
	streamNode *StreamNode
}

func NewKeyedStream(graph *Flow, Parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		graph:     graph,
		Parallel:  Parallel,
		Selector_: selector.NewKeyBy(keys),
		Operator_: operator.NewByPass(),
	}
}

func (s *KeyedStream) Selector() intfs.Selector {
	return s.Selector_.New()
}

func (s *KeyedStream) Operator() intfs.Operator {
	return s.Operator_.New()
}

func (s *KeyedStream) Rescale(Parallel int) *KeyedStream {
	s.Parallel = Parallel
	return s
}

func (s *KeyedStream) Parallelism() int {
	return s.Parallel
}

func (s *KeyedStream) SetStreamNode(node *StreamNode) {
	s.streamNode = node
}

func (s *KeyedStream) GetStreamNode() (node *StreamNode) {
	return s.streamNode
}

func (s *KeyedStream) toDataStream() *DataStream {
	return NewDataStream(s.graph, s.Parallel)
}

func (s *KeyedStream) toWindowedStream() *WindowedStream {
	return NewWindowedStream(s.graph, s.Parallel)
}

func (s *KeyedStream) connect(stream Stream) error {
	return s.graph.AddStreamEdge(s, stream)
}
