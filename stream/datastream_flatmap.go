package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *DataStream) FlatMap(fn funcintfs.FlatMap) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = fn
	s.connect(stream)
	return stream
}
