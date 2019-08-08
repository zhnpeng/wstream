package stream

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/runtime/operator"
)

func (s *KeyedStream) Reduce(reduceFunc functions.Reduce) *DataStream {
	stream := s.toDataStream()
	stream.operator = operator.NewReduce(reduceFunc)
	s.connect(stream)
	return stream
}
