package stream

import (
	"context"
	"encoding/gob"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/assigners"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/evictors"
	"github.com/zhnpeng/wstream/runtime/operator/windowing/triggers"
)

func init() {
	gob.Register(&WindowedStream{})
}

type WindowedStream struct {
	Parallel   int
	Assigner   assigners.WindowAssinger
	TheTrigger triggers.Trigger
	Evictor    evictors.Evictor

	ReduceFunc funcintfs.WindowReduce
	ApplyFunc  funcintfs.Apply

	flow *Flow
	fnid int
}

func NewWindowedStream(flow *Flow, parallel int) *WindowedStream {
	return &WindowedStream{
		flow:     flow,
		Parallel: parallel,
		fnid:     -1,
	}
}

func (s *WindowedStream) Trigger(trigger triggers.Trigger) *WindowedStream {
	s.TheTrigger = trigger
	return s
}

func (s *WindowedStream) Evict(evictor evictors.Evictor) *WindowedStream {
	s.Evictor = evictor
	return s
}

func (s *WindowedStream) Parallelism() int {
	return s.Parallel
}

func (s *WindowedStream) SetFlowNode(node *FlowNode) {
	s.fnid = node.ID
}

func (s *WindowedStream) GetFlowNode() (node *FlowNode) {
	return s.flow.GetFlowNode(s.fnid)
}

func (s *WindowedStream) toDataStream() *DataStream {
	return NewDataStream(s.flow, s.Parallel)
}

// toTask only work in local model
func (s *WindowedStream) toTask() *execution.Task {
	broadcastNodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		var optr operator.WindowOperator
		if s.Evictor != nil {
			optr = operator.NewEvictWindow(s.Assigner, s.TheTrigger, s.Evictor)
		} else {
			optr = operator.NewWindow(s.Assigner, s.TheTrigger)
		}
		optr.SetReduceFunc(s.ReduceFunc)
		optr.SetApplyFunc(s.ApplyFunc)
		node := execution.NewBroadcastNode(
			context.Background(),
			optr,
			execution.NewReceiver(),
			execution.NewEmitter(),
		)
		broadcastNodes = append(broadcastNodes, node)
	}
	return &execution.Task{
		BroadcastNodes: broadcastNodes,
	}
}

// combine new stream to windowed stream and then return new stream
func (s *WindowedStream) combine(stream Stream) {
	s.flow.CombineStream(s, stream)
}
