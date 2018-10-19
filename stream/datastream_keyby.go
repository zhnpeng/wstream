package stream

func (s *DataStream) KeyBy(keys ...interface{}) *KeyedStream {
	stream := s.toKeyedStream(keys)
	s.connect(stream)
	return stream
}
