package stream

import "github.com/wandouz/wstream/functions"

type Stream interface {
	Type() StreamType
	UDF() functions.UserDefinedFunction
	SetStreamNode(node *streamNode)
	GetStreamNode() (node *streamNode)
}
