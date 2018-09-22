package operator

import (
	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/types"
)

type Reduce struct {
	function    functions.ReduceFunc
	accumulator types.Record
}

func NewReduce(function functions.ReduceFunc) *Reduce {
	return &Reduce{
		function:    function,
		accumulator: function.InitialAccmulator(),
	}
}

func (m *Reduce) handleRecord(record types.Record, out Emitter) {
	m.accumulator = m.function.Reduce(m.accumulator, record)
	out.Emit(m.accumulator)
}

func (m *Reduce) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Reduce) Run(in *execution.Receiver, out Emitter) {
	consume(in, out, m)
}
