package stream

import "time"

func (s *KeyedStream) TimeWindow(every time.Duration) *DataStream {
	name := "time_window"
	graph := s.graph
	newStream := s.ToDataStream(name)
	graph.AddStreamEdge(s, newStream)

	return newStream
}
