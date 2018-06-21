package stream

type (
	FlatMapFunc func(item Event, out *Emitter)
)

type FlatMapTask struct {
	Function FlatMapFunc
}

func (t *FlatMapTask) Run(item Event, out *Emitter) {
	t.Function(item, out)
}

func (s *DataStream) FlatMap(flatMapFunc FlatMapFunc) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator

	// New Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	// New Task
	task := &FlatMapTask{
		Function: flatMapFunc,
	}
	operator.Task = task
	return ret
}