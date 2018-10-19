package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	stream := s.clone(name)
	stream.operator = operator.NewReduce(reduceFunc)
	s.connect(stream)
	return stream
}
