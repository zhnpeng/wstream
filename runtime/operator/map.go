package operator

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/types"
)

type Map struct {
	function functions.MapFunc
}

func (m *Map) handleRecord(record types.Record, out Emitter) {
	out.Emit(m.function.Map(record))
}

func (m *Map) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Map) Run(in *execution.Receiver, out Emitter) {
	consume(in, out, m)
}
