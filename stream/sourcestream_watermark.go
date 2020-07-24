package stream

import (
	"time"

	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *SourceStream) AssignTimeWithPeriodicWatermark(
	function funcintfs.AssignTimeWithPeriodicWatermark,
	period time.Duration,
) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = function
	stream.OperatorArgs = append(stream.OperatorArgs, period)
	s.connect(stream)
	return stream
}

func (s *SourceStream) AssignTimeWithPuncatuatedWatermark(
	function funcintfs.TimestampWithPunctuatedWatermark,
) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = function
	s.connect(stream)
	return stream
}
