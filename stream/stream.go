package stream

import "github.com/wandouz/wstream/runtime/execution"

type Stream interface {
	Parallelism() int
	Operator() execution.Operator
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}
