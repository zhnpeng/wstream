package stream

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator"

	"github.com/wandouz/wstream/functions"
)

func (s *SourceStream) AssignTimeWithPeriodicWatermark(
	function functions.AssignTimeWithPeriodicWatermark,
	period time.Duration,
) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewTimeWithPeriodicWatermarkAssigner(function, period)
	s.connect(stream)
	return stream
}

func (s *SourceStream) AssignTimeWithPuncatuatedWatermark(
	function functions.TimestampWithPunctuatedWatermar,
) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewTimeWithPunctuatedWatermarkAssigner(function)
	s.connect(stream)
	return stream
}