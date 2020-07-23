package stream

import (
	"context"
	"encoding/gob"
	"fmt"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/runtime/execution"
	"github.com/zhnpeng/wstream/runtime/operator"
	"github.com/zhnpeng/wstream/runtime/selector"
)

func init() {
	gob.Register(&DataStream{})
}

type DataStream struct {
	Parallel int

	// OperatorFunc is builtin functions, must be registed in gob
	// if OperatorFunc is nil, means a bypass operator
	OperatorFunc interface{}

	// StreamNode in flow's graph
	StreamNode *StreamNode

	// flow reference is not exported
	flow *Flow
}

/*
DataStream API
*/

func NewDataStream(flow *Flow, parallel int) *DataStream {
	return &DataStream{
		Parallel: parallel,
		flow:     flow,
	}
}

func (s *DataStream) Parallelism() int {
	return s.Parallel
}

func (s *DataStream) clone() *DataStream {
	return &DataStream{
		flow:     s.flow,
		Parallel: s.Parallel,
	}
}

func (s *DataStream) SetStreamNode(node *StreamNode) {
	s.StreamNode = node
}

func (s *DataStream) GetStreamNode() (node *StreamNode) {
	return s.StreamNode
}

func (s *DataStream) toKeyedStream(keys []interface{}) *KeyedStream {
	return NewKeyedStream(s.flow, s.Parallel, keys)
}

func (s *DataStream) toRescaleStream(parallel int, selector *selector.Selector) *RescaledStream {
	return NewRescaledStream(s.flow, parallel, selector)
}

func (s *DataStream) toTask() *execution.Task {
	broadcastNodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		var optr intfs.Operator
		switch theFunc := s.OperatorFunc.(type) {
		case funcintfs.Map:
			optr = operator.NewMap(theFunc)
		case funcintfs.FlatMap:
			optr = operator.NewFlatMap(theFunc)
		case funcintfs.Reduce:
			optr = operator.NewReduce(theFunc)
		case funcintfs.Output:
			optr = operator.NewOutput(theFunc)
		default:
			panic(fmt.Sprintf("not support %T", theFunc))
		}
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

func (s *DataStream) connect(stream Stream) error {
	return s.flow.AddStreamEdge(s, stream)
}

func (s *DataStream) Debug(debugFunc funcintfs.Debug) *DataStream {
	stream := s.clone()
	stream.OperatorFunc = debugFunc
	s.connect(stream)
	return stream
}
