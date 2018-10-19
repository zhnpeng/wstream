package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *KeyedStream) Reduce(reduceFunc functions.ReduceFunc) *DataStream {
	stream := s.toDataStream()
	stream.operator = operator.NewReduce(reduceFunc)
	s.connect(stream)
	return stream
}
