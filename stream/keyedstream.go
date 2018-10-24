package stream

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/operator"
)

type KeyedStream struct {
	parallel int
	selector intfs.Selector
	operator intfs.Operator

	graph      *Flow
	streamNode *StreamNode
}

func NewKeyedStream(graph *Flow, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		graph:    graph,
		parallel: parallel,
		selector: operator.NewKeyBy(keys),
		operator: operator.NewByPass(),
	}
}

func (s *KeyedStream) Selector() intfs.Selector {
	return s.selector.New()
}

func (s *KeyedStream) Operator() intfs.Operator {
	return s.operator.New()
}

func (s *KeyedStream) SetPartition(parallel int) *KeyedStream {
	s.parallel = parallel
	return s
}

func (s *KeyedStream) Parallelism() int {
	return s.parallel
}

func (s *KeyedStream) SetStreamNode(node *StreamNode) {
	s.streamNode = node
}

func (s *KeyedStream) GetStreamNode() (node *StreamNode) {
	return s.streamNode
}

func (s *KeyedStream) toDataStream() *DataStream {
	return NewDataStream(s.graph, s.parallel)
}

func (s *KeyedStream) toWindowedStream() *WindowedStream {
	return NewWindowedStream(s.graph, s.parallel)
}

func (s *KeyedStream) connect(stream Stream) {
	s.graph.AddStreamEdge(s, stream)
}
