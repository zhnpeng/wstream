package stream

import (
	"time"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *SourceStream) AssignTimeWithPeriodicWatermark(
	function funcintfs.AssignTimeWithPeriodicWatermark,
	period time.Duration,
) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewTimeWithPeriodicWatermarkAssigner(function, period)
	s.connect(stream)
	return stream
}

func (s *SourceStream) AssignTimeWithPuncatuatedWatermark(
	function funcintfs.TimestampWithPunctuatedWatermar,
) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewTimeWithPunctuatedWatermarkAssigner(function)
	s.connect(stream)
	return stream
}
