package stream

import "github.com/wandouz/wstream/functions"

type Stream interface {
	UDF() functions.UserDefinedFunction
	Parallelism() int
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}
