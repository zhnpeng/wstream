package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Map(mapFunc functions.MapFunc) *DataStream {
	name := "map"
	stream := s.clone(name)
	stream.operator = operator.NewMap(mapFunc)
	s.connect(stream)
	return stream
}
