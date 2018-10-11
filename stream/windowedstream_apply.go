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
	name := "reduce"
	graph := s.graph
	newStream := s.ToDataStream(name)
	wo := s.operator.(WindowOperator)
	wo.SetApplyFunc(applyFunc)
	// left merge new stream to windowed stream and then return new stream
	graph.LeftMergeStream(s, newStream)

	return newStream
}
