package stream

import (
	"context"
	"encoding/gob"
	"time"

	"github.com/pkg/errors"
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
	// OperatorFunc is builtin functions, must be registed in gob
	// if OperatorFunc is nil, means a bypass operator
	OperatorFunc interface{}
	OperatorArgs []interface{} // support basic type only
	Parallel     int

	flow *Flow // flow reference is not exported
	fnid int   // flow node id in flow's graph
}

/*
DataStream API
*/

func NewDataStream(flow *Flow, parallel int) *DataStream {
	return &DataStream{
		Parallel: parallel,
		flow:     flow,
		fnid:     -1,
	}
}

func (s *DataStream) Parallelism() int {
	return s.Parallel
}

func (s *DataStream) SetFlowNode(node *FlowNode) {
	s.fnid = node.ID
}

func (s *DataStream) GetFlowNode() (node *FlowNode) {
	return s.flow.GetFlowNode(s.fnid)
}

func (s *DataStream) toDataStream() *DataStream {
	return &DataStream{
		flow:     s.flow,
		Parallel: s.Parallel,
		fnid:     -1,
	}
}

func (s *DataStream) toKeyedStream(keys []interface{}) *KeyedStream {
	return NewKeyedStream(s.flow, s.Parallel, keys)
}

func (s *DataStream) toRescaleStream(parallel int, selector *selector.Selector) *RescaledStream {
	return NewRescaledStream(s.flow, parallel, selector)
}

func (s *DataStream) ToTask() *execution.Task {
	nodes := make([]execution.Node, 0, s.Parallelism())
	for i := 0; i < s.Parallelism(); i++ {
		var optr intfs.Operator
		if s.OperatorFunc == nil {
			optr = operator.NewByPass()
		} else {
			switch theFunc := s.OperatorFunc.(type) {
			case funcintfs.Map:
				optr = operator.NewMap(theFunc)
			case funcintfs.FlatMap:
				optr = operator.NewFlatMap(theFunc)
			case funcintfs.Reduce:
				optr = operator.NewReduce(theFunc)
			case funcintfs.Output:
				optr = operator.NewOutput(theFunc)
			case funcintfs.Debug:
				optr = operator.NewDebug(theFunc)
			case funcintfs.AssignTimeWithPeriodicWatermark:
				optr = operator.NewTimeWithPeriodicWatermarkAssigner(theFunc, s.OperatorArgs[0].(time.Duration))
			case funcintfs.TimestampWithPunctuatedWatermark:
				optr = operator.NewTimeWithPunctuatedWatermarkAssigner(theFunc)
			default:
				panic(errors.Errorf("Not Support Func Type: %T", theFunc))
			}
		}
		node := execution.NewExecutionNode(
			context.Background(),
			optr,
		)
		nodes = append(nodes, node)
	}
	return &execution.Task{
		Nodes: nodes,
	}
}

func (s *DataStream) connect(stream Stream) error {
	return s.flow.AddStreamEdge(s, stream)
}
