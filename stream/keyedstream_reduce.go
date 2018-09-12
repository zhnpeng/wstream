package stream

import "github.com/wandouz/wstream/functions"

func (s *KeyedStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	graph := s.graph
	newStream := s.ToDataStream(name, nil)
	graph.AddStreamEdge(s, newStream)

	newStream.udf = &functions.Reduce{
		Function: reduceFunc,
	}
	return newStream
}
