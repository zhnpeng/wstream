package stream

import (
	"fmt"
)

type DataStream struct {
	Stream
}

type (
	MapFunc func(In interface{}) (Out interface{})
)

func (s *DataStream) Copy() *DataStream {
	return &DataStream{
		Stream{
			OperatorGroups: s.OperatorGroups,
			Operator:       s.Operator,
			WaitGroup:      s.WaitGroup,
		},
	}
}

func (s *DataStream) Map(mapFunc MapFunc) *DataStream {
	ret := s.Copy()
	parentOperator := ret.Operator

	operator := NewOneToOneOperator(s.Operator)
	ret.Operator = operator

	parentOperator.Outgoing.Operators = append(
		parentOperator.Outgoing.Operators,
		operator,
	)

	outgoingOptGroup := &OperatorGroup{
		Operators: make([]*Operator, 0),
		Parent:    operator,
	}

	ret.OperatorGroups.Groups = append(ret.OperatorGroups.Groups, outgoingOptGroup)

	operator.Outgoing = outgoingOptGroup

	task := &Task{
		Operator: operator,
	}
	task.Function = func(in interface{}) interface{} {
		return mapFunc(in)
	}

	task.Operator = operator
	operator.Task = task
	return ret
}

func (s *DataStream) Printf(format string) *DataStream {
	ret := s.Copy()
	parentOperator := ret.Operator

	operator := NewOneToOneOperator(s.Operator)
	ret.Operator = operator

	parentOperator.Outgoing.Operators = append(
		parentOperator.Outgoing.Operators,
		operator,
	)

	outgoingOptGroup := &OperatorGroup{
		Operators: make([]*Operator, 0),
		Parent:    operator,
	}

	ret.OperatorGroups.Groups = append(ret.OperatorGroups.Groups, outgoingOptGroup)

	operator.Outgoing = outgoingOptGroup

	task := &Task{
		Operator: operator,
	}
	task.Function = func(in interface{}) interface{} {
		fmt.Printf(format+"%+v\n", in)
		return in
	}

	task.Operator = operator
	operator.Task = task
	return ret
}

func (s *DataStream) FlatMap() *DataStream {
	return nil
}

func (s *DataStream) Filter() *DataStream {
	return nil
}

func (s *DataStream) KeyBy() *KeyedStream {
	return nil
}
