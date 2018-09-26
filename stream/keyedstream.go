package stream

import (
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/operator"
)

type KeyedStream struct {
	name     string
	parallel int
	operator func() execution.Operator

	graph      *StreamGraph
	streamNode *StreamNode
}

func NewKeyedStream(name string, graph *StreamGraph, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		name:     name,
		graph:    graph,
		parallel: parallel,
		operator: operator.GenKeyBy(keys),
	}
}

func (s *KeyedStream) Operator() execution.Operator {
	return s.operator()
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

func (s *KeyedStream) ToDataStream(name string) *DataStream {
	return NewDataStream(
		name,
		s.graph,
		s.parallel,
	)
}
