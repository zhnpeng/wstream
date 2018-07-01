package stream


type (
	FilterFunc func(value Value) bool
)

type FilterTask struct {
	Function FilterFunc
}

func (t *FilterTask) Run(item Item, out *Emitter) {
	if item.Type() == TypeRecord {
		record := item.AsRecord()
		value := record.GetValue()
		if t.Function(value.Copy()) {
			out.Emit(item)
		}
	} else {
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