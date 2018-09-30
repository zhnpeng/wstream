package operator

import (
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// KeyBy is a rescaling node
type KeyBy struct {
	keys []interface{}
}

func NewKeyBy(keys []interface{}) *KeyBy {
	return &KeyBy{keys}
}
func (m *KeyBy) New() execution.Operator {
	return NewKeyBy(m.keys)
}

func (m *KeyBy) handleRecord(record types.Record, out utils.Emitter) {
	// usekeys and get key values
	kvs := record.UseKeys(m.keys)
	index := utils.PartitionByKeys(out.Length(), kvs)
	out.EmitTo(index, record)
}

func (m *KeyBy) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *KeyBy) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
