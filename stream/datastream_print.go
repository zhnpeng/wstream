package stream

import (
	"sync"
	"fmt"
)

type PrintfTask struct {
	Function func(i Event) Event
}

func (t *PrintfTask) Run(item Event, out *Emitter, wg *sync.WaitGroup) {
	v := t.Function(item)
	out.Emit(v)
}

func (s *DataStream) Printf(format string) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	task := &PrintfTask{}
	task.Function = func(i Event) Event {
		fmt.Printf(format+"%+v\n", i)
		return i
	}
	operator.Task = task
	return ret
}