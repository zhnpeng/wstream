package stream

import (
	"github.com/zhnpeng/wstream/funcintfs"
)

func (s *KeyedStream) Reduce(reduceFunc funcintfs.Reduce) *DataStream {
	stream := s.toDataStream()
	stream.OperatorFunc = reduceFunc
	s.connect(stream)
	return stream
}
