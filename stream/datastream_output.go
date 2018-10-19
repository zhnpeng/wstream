package stream

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/operator"
)

func (s *DataStream) Output(outputFunc functions.OutputFunc) *DataStream {
	name := "output"
	graph := s.graph
	newStream := s.Copy(name)
	graph.AddStreamEdge(s, newStream)

	newStream.operator = operator.NewOutput(outputFunc)
	return newStream
}
