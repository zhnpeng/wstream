package operator

import (
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type ByPass struct {
}

func NewByPass() *ByPass {
	return &ByPass{}
}

func (m *ByPass) New() intfs.Operator {
	return NewByPass()
}

func (m *ByPass) handleRecord(record types.Record, out Emitter) {
	out.Emit(record)
}

func (m *ByPass) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *ByPass) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
