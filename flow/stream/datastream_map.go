package stream

import (
	"github.com/wandouz/wstream/flow/functions"
)

func (s *DataStream) Map(mapFunc functions.MapFunc) *DataStream {
	name := "map"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.udf = &functions.Map{
		Function: mapFunc,
	}
	return newStream
}
