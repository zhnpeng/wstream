package stream

import (
	"fmt"
)

type PrintfTask struct {
	Function func(i Item) Item
}

func (t *PrintfTask) Run(item Item, out *Emitter) {
	t.Function(item)
	out.Emit(item)
}

func (s *DataStream) Printf(format string) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	task := &PrintfTask{
		Function: func(i Item) Item {
			if i.Type() == TypeRecord {
				fmt.Printf(format+"%s\n", i.AsRecord().GetValue().AsString())
			}
			return i
		},
	}
	operator.Task = task
	return ret
}
