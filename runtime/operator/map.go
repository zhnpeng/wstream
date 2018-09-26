package operator

import (
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Map struct {
	function functions.MapFunc
}

func GenMap(function functions.MapFunc) func() execution.Operator {
	// use gob to support multi instance of user defined function
	reader := encodeFunction(function)
	return func() (ret execution.Operator) {
		defer reader.Seek(0, 0)
		decoder := gob.NewDecoder(reader)
		var udf functions.MapFunc
		decoder.Decode(&udf)
		ret = NewMap(udf)
		return
	}
}

func NewMap(function functions.MapFunc) *Map {
	return &Map{function}
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
