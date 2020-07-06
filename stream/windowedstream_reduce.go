package stream

import (
	"github.com/zhnpeng/wstream/functions"
)

func (s *WindowedStream) Reduce(reduceFunc functions.WindowReduce) *DataStream {
	stream := s.toDataStream()
	operator := s.operator.(WindowOperator)
	operator.SetReduceFunc(reduceFunc)
	s.leftMerge(stream)
	return stream
}
