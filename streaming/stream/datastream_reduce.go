package stream

import (
	"github.com/wandouz/wstream/streaming/functions"
)

func (s *DataStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.udf = &functions.Reduce{
		Function: reduceFunc,
	}
	return newStream
}
