package stream

import (
	"context"
	"encoding/gob"

	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/selector"
)

func init() {
	gob.Register(&KeyedStream{})
}

type KeyedStream struct {
	Parallel int
	Selector intfs.Selector

	graph    *Flow
	FlowNode *FlowNode
}

func NewKeyedStream(graph *Flow, Parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		graph:    graph,
		Parallel: Parallel,
		Selector: selector.NewKeyBy(keys),
	}
}

func (s *KeyedStream) Rescale(Parallel int) *KeyedStream {
	s.Parallel = Parallel
	return s
}

func (s *KeyedStream) Parallelism() int {
	return s.Parallel
}

func (s *KeyedStream) SetFlowNode(node *FlowNode) {
	s.FlowNode = node
}

func (s *KeyedStream) GetFlowNode() (node *FlowNode) {
	return s.FlowNode
}

func (s *KeyedStream) toDataStream() *DataStream {
	return NewDataStream(s.graph, s.Parallel)
}

func (s *KeyedStream) toWindowedStream() *WindowedStream {
	return NewWindowedStream(s.graph, s.Parallel)
}

func (s *KeyedStream) toTask() *execution.Task {
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

func (s *KeyedStream) connect(stream Stream) error {
	return s.graph.AddStreamEdge(s, stream)
}
