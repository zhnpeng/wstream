package operator

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

// KeyBy is a rescaling node
type KeyBy struct {
	keys []interface{}
}

func NewKeyBy(keys []interface{}) *KeyBy {
	return &KeyBy{
		keys: keys,
	}
}
func (m *KeyBy) New() intfs.Operator {
	return NewKeyBy(m.keys)
}

func (m *KeyBy) handleRecord(record types.Record, out Emitter) {
	// usekeys and get key values
	kvs := record.UseKeys(m.keys...)
	index := utils.PartitionByKeys(out.Length(), kvs)
	out.EmitTo(index, record)
}

func (m *KeyBy) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *KeyBy) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
