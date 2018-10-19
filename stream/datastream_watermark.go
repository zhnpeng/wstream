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
	stream := s.clone()
	stream.operator = operator.NewAssignTimestampWithPeriodicWatermark(function, period)
	s.connect(stream)
	return stream
}

func (s *DataStream) TimestampWithPuncatuatedWatermark(
	function functions.TimestampWithPunctuatedWatermar,
) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewAssignTimestampWithPunctuatedWatermark(function)
	s.connect(stream)
	return stream
}
