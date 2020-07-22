package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *DataStream) FlatMap(fn funcintfs.FlatMap) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewFlatMap(fn)
	s.connect(stream)
	return stream
}
