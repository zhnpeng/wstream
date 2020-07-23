package stream

import "github.com/zhnpeng/wstream/funcintfs"

func (s *WindowedStream) Apply(applyFunc funcintfs.Apply) *DataStream {
	stream := s.toDataStream()
	s.ApplyFunc = applyFunc
	s.combine(stream)
	return stream
}
