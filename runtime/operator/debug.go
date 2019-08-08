package operator

import (
	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type Debug struct {
	function functions.Debug
}

func NewDebug(function functions.Debug) *Debug {
	if function == nil {
		panic("Debug function must not be nil")
	}
	return &Debug{function}
}

func (m *Debug) New() intfs.Operator {
	return NewDebug(m.function)
}

func (m *Debug) handleRecord(record types.Record, out Emitter) {
	m.function.Debug(record)
	out.Emit(record)
}

func (m *Debug) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Debug) Run(in Receiver, out Emitter) {
	for {
		item, ok := <-in.Next()
		if !ok {
			return
		}
		switch item.(type) {
		case types.Record:
			m.handleRecord(item.(types.Record), out)
		case *types.Watermark:
			m.handleWatermark(item.(*types.Watermark), out)
		}
	}
}
