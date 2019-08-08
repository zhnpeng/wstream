package operator

import (
	"sync/atomic"

	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

// SourceRoundrobin use in source stream
// use to rescale records to down stream in roundrobin way
type SourceRoundrobin struct {
	count int64
}

func NewSourceRoundrobin() *SourceRoundrobin {
	return &SourceRoundrobin{}
}

func (m *SourceRoundrobin) New() intfs.Operator {
	return NewSourceRoundrobin()
}

func (m *SourceRoundrobin) handleRecord(record types.Record, out Emitter) {
	// TODO: refine this
	// Round Robin way to emit item
	// get key values, then calculate index, then emit to partition by index
	cnt := atomic.AddInt64(&m.count, 1)
	index := cnt % int64(out.Length())
	_ = out.EmitTo(int(index), record)
}

func (m *SourceRoundrobin) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *SourceRoundrobin) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
