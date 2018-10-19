package stream

import (
	"github.com/wandouz/wstream/functions"
)

// WindowOperator is helper interface to set user functions to window operator
type WindowOperator interface {
	SetApplyFunc(functions.ApplyFunc)
	SetReduceFunc(functions.ReduceFunc)
}

func (s *WindowedStream) Apply(applyFunc functions.ApplyFunc) *DataStream {
	stream := s.toDataStream()
	operator := s.operator.(WindowOperator)
	operator.SetApplyFunc(applyFunc)
	s.leftMerge(stream)
	return stream
}
