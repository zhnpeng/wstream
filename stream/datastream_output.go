package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Output(outputFunc functions.OutputFunc) *DataStream {
	name := "output"
	stream := s.clone(name)
	stream.operator = operator.NewOutput(outputFunc)
	s.connect(stream)
	return stream
}
