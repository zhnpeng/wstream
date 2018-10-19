package stream

func (s *DataStream) KeyBy(keys ...interface{}) *KeyedStream {
	name := "keyby"
	stream := s.toKeyedStream(name, keys)
	s.connect(stream)
	return stream
}

// ToKeyedStream TODO: refine this
func (s *DataStream) toKeyedStream(name string, keys []interface{}) *KeyedStream {
	return NewKeyedStream(name, s.graph, s.parallel, keys)
}
