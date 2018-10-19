package stream

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/runtime/operator"
)

type KeyedStream struct {
	name     string
	parallel int
	operator intfs.Operator

	graph      *StreamGraph
	streamNode *StreamNode
}

func NewKeyedStream(name string, graph *StreamGraph, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		name:     name,
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

func (s *KeyedStream) toDataStream(name string) *DataStream {
	return NewDataStream(
		name,
		s.graph,
		s.parallel,
	)
}

func (s *KeyedStream) toWindowedStream(name string) *WindowedStream {
	return NewWindowedStream(
		name,
		s.graph,
		s.parallel,
	)
}

func (s *KeyedStream) connect(stream Stream) {
	s.graph.AddStreamEdge(s, stream)
}
