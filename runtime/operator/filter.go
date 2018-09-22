package operator

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/types"
)

type Filter struct {
	function functions.FilterFunc
}

func (m *Filter) handleRecord(record types.Record, out Emitter) {
	if m.function.Filter(record) {
		out.Emit(record)
	}
}

func (m *Filter) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Filter) Run(in *execution.Receiver, out Emitter) {
	consume(in, out, m)
}
