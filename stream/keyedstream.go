package stream

import (
	"encoding/gob"

	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/selector"
)

func init() {
	gob.Register(&KeyedStream{})
}

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
		selector: selector.NewKeyBy(keys),
		operator: operator.NewByPass(),
	}
}

func (s *KeyedStream) Selector() intfs.Selector {
	return s.selector.New()
}

func (s *KeyedStream) Operator() intfs.Operator {
	return s.operator.New()
}

func (s *KeyedStream) Rescale(parallel int) *KeyedStream {
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

func (s *KeyedStream) connect(stream Stream) error {
	return s.graph.AddStreamEdge(s, stream)
}
