package operator

import (
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type KeyBy struct {
	keys []interface{}
}

func (m *KeyBy) handleRecord(record types.Record, out Emitter) {
	// get key values, then calculate index, then emit to partition by index
	kvs := record.GetMany(m.keys)
	index := utils.PartitionByKeys(out.Length(), kvs)
	out.EmitTo(index, record)
}

func (m *KeyBy) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *KeyBy) Run(in *execution.Receiver, out Emitter) {
	dispatch(in, out, m)
}
