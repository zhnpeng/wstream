package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Map(mapFunc functions.MapFunc) *DataStream {
	name := "map"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.operator = operator.NewMap(mapFunc)
	return newStream
}
