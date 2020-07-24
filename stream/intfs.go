package stream

type Stream interface {
	Parallelism() int
	SetFlowNode(node *FlowNode)
	GetFlowNode() (node *FlowNode)
}
