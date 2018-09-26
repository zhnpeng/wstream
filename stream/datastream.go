package stream

import (
	"github.com/wandouz/wstream/runtime/execution"
)

type DataStream struct {
	name     string
	parallel int
	operator func() execution.Operator

	options map[string]interface{}

	// graph reference
	streamNode *StreamNode
	graph      *StreamGraph
}

/*
DataStream API
*/

func NewDataStream(name string, graph *StreamGraph, parallel int, options map[string]interface{}) *DataStream {
	return &DataStream{
		name:     name,
		parallel: parallel,
		graph:    graph,
		options:  options,
	}
}

func (s *DataStream) Operator() execution.Operator {
	return s.operator()
}

func (s *DataStream) Parallelism() int {
	return s.parallel
}

func (s *DataStream) Copy(name string) *DataStream {
	return &DataStream{
		name:     name,
		graph:    s.graph,
		parallel: s.parallel,
	}
}

func (s *DataStream) SetPartition(parallel int) *DataStream {
	s.parallel = parallel
	return s
}

func (s *DataStream) SetStreamNode(node *StreamNode) {
	s.streamNode = node
}

func (s *DataStream) GetStreamNode() (node *StreamNode) {
	return s.streamNode
}
