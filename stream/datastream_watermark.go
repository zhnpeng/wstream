package stream

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator"

	"github.com/wandouz/wstream/functions"
)

func (s *DataStream) TimestampWithPeriodicWatermark(
	function functions.TimestampWithPeriodicWatermark,
	period time.Duration,
) *DataStream {
	name := "assignTimestampWithPeriodicWatermark"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.operator = operator.NewAssignTimestampWithPeriodicWatermark(function, period)
	return newStream
}

func (s *DataStream) TimestampWithPuncatuatedWatermark(
	function functions.TimestampWithPunctuatedWatermar,
) *DataStream {
	name := "assignTimestampWithPuncatuatedWatermark"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.operator = operator.NewAssignTimestampWithPunctuatedWatermark(function)
	return newStream
}
