package stream

import (
	"github.com/wandouz/wstream/functions"
)

// WindowOperator is helper interface to set user functions to window operator
type WindowOperator interface {
	SetApplyFunc(functions.Apply)
	SetReduceFunc(functions.Reduce)
}

func (s *WindowedStream) Apply(applyFunc functions.Apply) *DataStream {
	stream := s.toDataStream()
	operator := s.operator.(WindowOperator)
	operator.SetApplyFunc(applyFunc)
	s.leftMerge(stream)
	return stream
}
