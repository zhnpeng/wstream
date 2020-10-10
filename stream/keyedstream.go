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

	flow *Flow
	fnid int
}

func NewKeyedStream(flow *Flow, Parallel int, keys []interface{}) *KeyedStream {
	return &KeyedStream{
		flow:     flow,
		Parallel: Parallel,
		Selector: selector.NewKeyBy(keys),
		fnid:     -1,
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
	s.fnid = node.ID
}

func (s *KeyedStream) GetFlowNode() (node *FlowNode) {
	return s.flow.GetFlowNode(s.fnid)
}

func (s *KeyedStream) toDataStream() *DataStream {
	return NewDataStream(s.flow, s.Parallel)
}

func (s *KeyedStream) toWindowedStream() *WindowedStream {
	return NewWindowedStream(s.flow, s.Parallel)
}

func (s *KeyedStream) ToTask() *execution.Task {
	nodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		node := execution.NewRawExecutionNode(
			context.Background(),
			operator.NewRescale(s.Selector),
			execution.NewReceiver(),
			execution.NewGroupEmitter(),
		)
		nodes = append(nodes, node)
	}
	return &execution.Task{
		Nodes: nodes,
	}
}

func (s *KeyedStream) connect(stream Stream) error {
	return s.flow.AddStreamEdge(s, stream)
}
