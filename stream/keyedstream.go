package stream

type KeyedStream struct {
	Stream
}

func (s *KeyedStream) Window() *WindowedStream {
	return nil
}

func (s *KeyedStream) Reduce() *DataStream {
	return nil
}

func (s *KeyedStream) Sum() *DataStream {
	return nil
}
