package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *DataStream) Reduce(reduceFunc functions.Reduce) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewReduce(reduceFunc)
	s.connect(stream)
	return stream
}
