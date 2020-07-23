package stream

import (
	"time"

	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *SourceStream) AssignTimeWithPeriodicWatermark(
	function funcintfs.AssignTimeWithPeriodicWatermark,
	period time.Duration,
) *DataStream {
	stream := s.clone()
	stream.OperatorFunc = function
	s.connect(stream)
	return stream
}

func (s *SourceStream) AssignTimeWithPuncatuatedWatermark(
	function funcintfs.TimestampWithPunctuatedWatermar,
) *DataStream {
	stream := s.clone()
	stream.OperatorFunc = function
	s.connect(stream)
	return stream
}
