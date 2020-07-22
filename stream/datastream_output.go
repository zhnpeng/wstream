package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *DataStream) Output(outputFunc funcintfs.Output) *DataStream {
	stream := s.clone()
	stream.operator = operator.NewOutput(outputFunc)
	s.connect(stream)
	return stream
}
