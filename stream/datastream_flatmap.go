package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *DataStream) FlatMap(fn functions.FlatMap) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewFlatMap(fn)
	s.connect(stream)
	return stream
}
