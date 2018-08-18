package stream

import "github.com/wandouz/wstream/streaming/functions"

type Stream interface {
	Type() StreamType
	UDF() functions.UserDefinedFunction
	SetStreamNode(node *streamNode)
	GetStreamNode() (node *streamNode)
}

//Basic is base stream struct
type Basic struct {
	name       string
	parallel   int
	streamNode *streamNode
	graph      *StreamGraph
	options    map[string]interface{}
}

/*
Graph API
*/

func (s *Basic) UDF() functions.UserDefinedFunction {
	return nil
}

func (s *Basic) SetStreamNode(node *streamNode) {
	s.streamNode = node
}

func (s *Basic) GetStreamNode() (node *streamNode) {
	return s.streamNode
}
