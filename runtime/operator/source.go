package operator

import (
	"sync/atomic"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/types"
)

type Source struct {
	count int64
}

func (m *Source) handleRecord(record types.Record, out Emitter) {
	// Round Robin way to emit item
	// get key values, then calculate index, then emit to partition by index
	cnt := atomic.AddInt64(&m.count, 1)
	index := cnt % int64(out.Length())
	out.EmitTo(int(index), record)
}

func (m *Source) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Source) Run(in *execution.Receiver, out Emitter) {
	consume(in, out, m)
}
