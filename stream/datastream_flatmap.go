package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) FlatMap(fn functions.FlatMap) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewFlatMap(fn)
	s.connect(stream)
	return stream
}
