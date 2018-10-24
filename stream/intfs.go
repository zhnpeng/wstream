package stream

type Stream interface {
	Parallelism() int
	SetStreamNode(node *StreamNode)
	GetStreamNode() (node *StreamNode)
}
