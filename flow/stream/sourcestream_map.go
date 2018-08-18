package stream

import (
	"github.com/wandouz/wstream/streaming/functions"
)

func (s *SourceStream) Map(mapFunc functions.MapFunc) *DataStream {
	name := "map"
	graph := s.graph
	newStream := s.ToDataStream(name, nil)
	graph.AddStreamEdge(s, newStream)

	newStream.udf = &functions.Map{
		Function: mapFunc,
	}

	return newStream
}

func (s *SourceStream) ToDataStream(name string, options map[string]interface{}) *DataStream {
	return NewDataStream(
		name,
		s.graph,
		s.parallel,
		options,
	)
}
