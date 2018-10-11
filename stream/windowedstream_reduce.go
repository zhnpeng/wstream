package stream

import (
	"github.com/wandouz/wstream/functions"
)

func (s *WindowedStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	name := "reduce"
	graph := s.graph
	newStream := s.ToDataStream(name)
	wo := s.operator.(WindowOperator)
	wo.SetReduceFunc(reduceFunc)
	graph.LeftMergeStream(s, newStream)

	return newStream
}
