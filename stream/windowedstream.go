package stream

type WindowedStream struct{
	Stream
}

func (s *WindowedStream) Apply() *DataStream {
	return nil
}

func (s *WindowedStream) Reduce() *DataStream {
	return nil
}

func (s *WindowedStream) Sum() *DataStream {
	return nil
}