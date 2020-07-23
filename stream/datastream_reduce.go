package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *DataStream) Reduce(reduceFunc funcintfs.Reduce) *DataStream {
	stream := s.clone()
	stream.OperatorFunc = reduceFunc
	s.connect(stream)
	return stream
}
