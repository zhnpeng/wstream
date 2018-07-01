package stream

//RecordCollector collect value for flatmap and/or apply like operator
//It is a closure of emitter and record,
//When Emit function recive a Value, record create a copy
//of itselt with this Value and emit to emitter
type RecordCollector struct {
	emitter *Emitter
	record  *Record
}

func NewRecordCollector(record *Record, emitter *Emitter) *RecordCollector {
	return &RecordCollector{
		record:  record,
		emitter: emitter,
	}
}

func (c *RecordCollector) Emit(value Value) {
	newItem := c.record.Copy(value)
	c.emitter.Emit(newItem)
}