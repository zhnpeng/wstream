package stream

import "github.com/wandouz/wstream/functions"

type KeyedStream struct {
	name       string
	parallel   int
	streamNode *StreamNode
	graph      *StreamGraph
	options    map[string]interface{}
	keys       []interface{}
}

func NewKeyedStream(name string, graph *StreamGraph, parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		name:     name,
		graph:    graph,
		parallel: parallel,
		keys:     keys,
	}
}

func (s *KeyedStream) Type() StreamType {
	return TypeKeyedStream
}

func (s *KeyedStream) UDF() functions.UserDefinedFunction {
	return nil
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
