package stream

import (
	"context"

	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/runtime/operator"
)

type RescaledStream struct {
	DataStream
	Selector intfs.Selector
}

func NewRescaledStream(flow *Flow, parallel int, selector intfs.Selector) *RescaledStream {
	stm := &RescaledStream{
		DataStream: DataStream{
			flow: flow,
		},
		Selector: selector,
	}
	return stm
}

func (s *RescaledStream) toTask() *execution.Task {
	nodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		node := execution.NewExecutionNode(
			context.Background(),
			operator.NewRescale(s.Selector),
		)
		edge := make(execution.Edge)
		node.AddInEdge(edge.In())
		nodes = append(nodes, node)
	}
	return &execution.Task{
		Nodes: nodes,
	}
}
