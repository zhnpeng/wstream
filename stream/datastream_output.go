package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Output(outputFunc functions.Output) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewOutput(outputFunc)
	s.connect(stream)
	return stream
}
