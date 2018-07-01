package stream

type (
	FlatMapFunc func(value Value, out *RecordCollector)
)

type FlatMapTask struct {
	Function FlatMapFunc
}

func (t *FlatMapTask) Run(item Item, out *Emitter) {
	if item.Type() == TypeRecord {
		record := item.AsRecord()
		collector := NewRecordCollector(record, out)
		t.Function(record.GetValue(), collector)
	} else {
		out.Emit(item)
	}
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
