package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *DataStream) Output(outputFunc funcintfs.Output) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = outputFunc
	s.connect(stream)
	return stream
}
