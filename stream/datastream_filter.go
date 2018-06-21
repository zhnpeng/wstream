package stream


type (
	FilterFunc func(item Event) bool
)

type FilterTask struct {
	Function FilterFunc
}

func (t *FilterTask) Run(item Event, out *Emitter) {
	if t.Function(item) {
		out.Emit(item)
	}
}

func (s *DataStream) Filter(filterFunc FilterFunc) *DataStream {
	ret := s.Copy()
	parentOperator := s.Operator

	// New Operator
	operator := NewOneToOneOperator(ret.Graph, parentOperator)
	ret.Operator = operator

	// New Task
	task := &FilterTask{
		Function: filterFunc,
	}
	operator.Task = task
	return ret
}