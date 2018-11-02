package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Map(mapFunc functions.Map) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewMap(mapFunc)
	s.connect(stream)
	return stream
}
