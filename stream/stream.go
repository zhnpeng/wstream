package stream

import "github.com/wandouz/wstream/intfs"

type Stream interface {
	Parallelism() int
	Operator() intfs.Operator
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}
