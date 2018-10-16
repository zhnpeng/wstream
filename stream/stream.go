package stream

import "github.com/wandouz/wstream/runtime/utils"

type Stream interface {
	Parallelism() int
	Operator() utils.Operator
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}
