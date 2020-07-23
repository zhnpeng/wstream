package stream

import "github.com/zhnpeng/wstream/funcintfs"

func (s *WindowedStream) Reduce(reduceFunc funcintfs.WindowReduce) *DataStream {
	stream := s.toDataStream()
	s.ReduceFunc = reduceFunc
	s.combine(stream)
	return stream
}
