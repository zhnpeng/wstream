package operator

import (
	"sync/atomic"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type RescaleRoundrobin struct {
	count int64
}

func NewRescaleRoundRobin() *RescaleRoundrobin {
	return &RescaleRoundrobin{}
}

func (m *RescaleRoundrobin) New() execution.Operator {
	return NewRescaleRoundRobin()
}

func (m *RescaleRoundrobin) handleRecord(record types.Record, out utils.Emitter) {
	// TODO: refine this
	// Round Robin way to emit item
	// get key values, then calculate index, then emit to partition by index
	cnt := atomic.AddInt64(&m.count, 1)
	index := cnt % int64(out.Length())
	out.EmitTo(int(index), record)
}

func (m *RescaleRoundrobin) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *RescaleRoundrobin) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
