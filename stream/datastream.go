package stream

import "github.com/wandouz/wstream/flow/functions"

type DataStream struct {
	name     string
	parallel int
	options  map[string]interface{}

	udf functions.UserDefinedFunction

	// graph reference
	streamNode *streamNode
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

func (s *DataStream) Type() StreamType {
	return TypeDataStream
}

func (s *DataStream) UDF() functions.UserDefinedFunction {
	return s.udf
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

func (s *DataStream) SetStreamNode(node *streamNode) {
	s.streamNode = node
}

func (s *DataStream) GetStreamNode() (node *streamNode) {
	return s.streamNode
}
