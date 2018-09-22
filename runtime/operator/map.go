package operator

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Map struct {
	function functions.MapFunc
}

func (m *Map) handleRecord(record types.Record, out utils.Emitter) {
	out.Emit(m.function.Map(record))
}

func (m *Map) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *Map) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
