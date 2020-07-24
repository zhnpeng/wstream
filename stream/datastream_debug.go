package stream

import "github.com/zhnpeng/wstream/funcintfs"

func (s *DataStream) Debug(debugFunc funcintfs.Debug) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = debugFunc
	s.connect(stream)
	return stream
}
