package stream

func (s *DataStream) KeyBy(keys ...interface{}) *KeyedStream {
	name := "keyby"
	graph := s.graph
	newStream := s.ToKeyedStream(name, keys)
	graph.AddStreamEdge(s, newStream)

	return newStream
}

// ToKeyedStream TODO: refine this
func (s *DataStream) ToKeyedStream(name string, keys []interface{}) *KeyedStream {
	return NewKeyedStream(name, s.graph, s.parallel, keys)
}
