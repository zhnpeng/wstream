package stream

type (
	MapFunc func(item Event) (Out Event)
)

type MapTask struct {
	Function MapFunc
}

func (t *MapTask) Run(item Event, out *Emitter) {
	v := t.Function(item)
	out.Emit(v)
}

func (s *DataStream) Map(mapFunc MapFunc) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator

	// New Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	// New Task
	task := &MapTask{
		Function: mapFunc,
	}
	operator.Task = task
	return ret
}
