package stream

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/operator"
)

type KeyedStream struct {
	parallel int
	operator intfs.Operator

	graph      *Flow
	streamNode *StreamNode
}

func NewKeyedStream(graph *Flow, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		graph:    graph,
		parallel: parallel,
		operator: operator.NewKeyBy(keys),
	}
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
