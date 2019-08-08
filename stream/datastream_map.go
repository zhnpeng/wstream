package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *DataStream) Map(mapFunc functions.Map) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewMap(mapFunc)
	s.connect(stream)
	return stream
}
