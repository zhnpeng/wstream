package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.operator = operator.NewReduce(reduceFunc)
	return newStream
}
