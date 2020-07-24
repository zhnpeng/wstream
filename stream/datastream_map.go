package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *DataStream) Map(mapFunc funcintfs.Map) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = mapFunc
	s.connect(stream)
	return stream
}
