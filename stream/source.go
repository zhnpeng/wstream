package stream

import (
	"sync"
)

func NewChannelStream(input chan interface{}) *DataStream {
	channels := []chan interface{}{input}
	var wg sync.WaitGroup
	ret := &DataStream{
		Stream{
			WaitGroup:     &wg,
			OperatorGroups: &OperatorGroupSlice{Groups: make([]*OperatorGroup, 0)},
		},
	}
	// New Operator
	operator := NewOperator(channels)
	ret.Operator = operator

	optGroup := &OperatorGroup{
		Operators: []*Operator{operator},
	}
	outgoingOptGroup := &OperatorGroup{
		Operators: make([]*Operator, 0),
		Parent:    operator,
	}

	operator.Outgoing = outgoingOptGroup
	ret.OperatorGroups.Groups = append(ret.OperatorGroups.Groups, optGroup, outgoingOptGroup)

	task := &Task{
		Operator: operator,
	}
	task.Function = func(in interface{}) interface{} {
		return in
	}

	task.Operator = operator
	operator.Task = task
	return ret
}
