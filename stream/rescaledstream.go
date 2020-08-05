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
	rescaleNode := execution.NewRescaleNode(
		context.Background(),
		s.Selector,
	)
	broadcastNodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		optr := operator.NewByPass()
		broadcastNode := execution.NewBroadcastNode(
			context.Background(),
			optr,
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		edge := make(execution.Edge)
		rescaleNode.AddOutEdge(edge.Out())
		broadcastNode.AddInEdge(edge.In())
		broadcastNodes = append(broadcastNodes, broadcastNode)
	}
	return &execution.Task{
		RescaleNode:    rescaleNode,
		BroadcastNodes: broadcastNodes,
	}
}
