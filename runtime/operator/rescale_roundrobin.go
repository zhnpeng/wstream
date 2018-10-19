package operator

import (
	"sync/atomic"

	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type RescaleRoundrobin struct {
	count int64
}

func NewRescaleRoundRobin() *RescaleRoundrobin {
	return &RescaleRoundrobin{}
}

func (m *RescaleRoundrobin) New() intfs.Operator {
	return NewRescaleRoundRobin()
}

func (m *RescaleRoundrobin) handleRecord(record types.Record, out Emitter) {
	// TODO: refine this
	// Round Robin way to emit item
	// get key values, then calculate index, then emit to partition by index
	cnt := atomic.AddInt64(&m.count, 1)
	index := cnt % int64(out.Length())
	out.EmitTo(int(index), record)
}

func (m *RescaleRoundrobin) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *RescaleRoundrobin) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
