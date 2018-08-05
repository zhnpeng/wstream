package stream

import "github.com/wandouz/wstream/streaming/functions"

type DataStream struct {
	Basic
	udf functions.UserDefinedFunction
}

/*
DataStream API
*/

func NewDataStream(name string, graph *StreamGraph, parallel int, options map[string]interface{}) *DataStream {
	return &DataStream{
		Basic: Basic{
			name:     name,
			parallel: parallel,
			graph:    graph,
			options:  options,
		},
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
		Basic: Basic{
			name:     name,
			graph:    s.graph,
			parallel: s.parallel,
		},
	}
}

func (s *DataStream) SetPartition(parallel int) *DataStream {
	s.parallel = parallel
	return s
}
