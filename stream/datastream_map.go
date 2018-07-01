package stream

type (
	MapFunc func(item Value) (Out Value)
)

type MapTask struct {
	Function MapFunc
}

func (t *MapTask) Run(item Item, out *Emitter) {
	if item.Type() == TypeRecord {
		record := item.AsRecord()
		v := t.Function(record.GetValue())
		out.Emit(record.Copy(v))
	} else {
		out.Emit(item)
	}
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
