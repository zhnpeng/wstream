package stream

import "time"

func (s *KeyedStream) TimeWindow(every time.Duration) *DataStream {
	name := "time_window"
	graph := s.graph
	options := map[string]interface{}{
		"period": every,
		"every":  every,
	}
	newStream := s.ToDataStream(name, options)
	graph.AddStreamEdge(s, newStream)

	return newStream
}

func (s *KeyedStream) ToDataStream(name string, options map[string]interface{}) *DataStream {
	return NewDataStream(
		name,
		s.graph,
		s.parallel,
		options,
	)
}
