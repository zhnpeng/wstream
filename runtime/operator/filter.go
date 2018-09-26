package operator

import (
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Filter struct {
	function functions.FilterFunc
}

func GenFilter(function functions.FilterFunc) func() execution.Operator {
	reader := encodeFunction(function)
	return func() (ret execution.Operator) {
		defer reader.Seek(0, 0)
		decoder := gob.NewDecoder(reader)
		var udf functions.FilterFunc
		decoder.Decode(&udf)
		ret = NewFilter(udf)
		return
	}
}

func NewFilter(function functions.FilterFunc) *Filter {
	return &Filter{function}
}

func (m *Filter) handleRecord(record types.Record, out utils.Emitter) {
	if m.function.Filter(record) {
		out.Emit(record)
	}
}

func (m *Filter) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *Filter) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
