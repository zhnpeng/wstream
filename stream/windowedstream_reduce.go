package stream

import (
	"github.com/wandouz/wstream/functions"
)

func (s *WindowedStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	stream := s.toDataStream(name)
	operator := s.Operator().(WindowOperator)
	operator.SetReduceFunc(reduceFunc)
	s.leftMerge(stream)
	return stream
}
