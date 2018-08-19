package stream

import "github.com/wandouz/wstream/flow/functions"

type Stream interface {
	Type() StreamType
	UDF() functions.UserDefinedFunction
	SetStreamNode(node *streamNode)
	GetStreamNode() (node *streamNode)
}
