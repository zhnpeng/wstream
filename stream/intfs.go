package stream

import "github.com/zhnpeng/wstream/runtime/execution"

type Stream interface {
	Parallelism() int
	SetFlowNode(node *FlowNode)
	GetFlowNode() (node *FlowNode)
	ToTask() *execution.Task
}
