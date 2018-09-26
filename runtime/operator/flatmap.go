package operator

import (
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type FlatMap struct {
	function functions.FlatMapFunc
}

func GenFlatMap(function functions.FlatMapFunc) func() execution.Operator {
	reader := encodeFunction(function)
	return func() (ret execution.Operator) {
		defer reader.Seek(0, 0)
		decoder := gob.NewDecoder(reader)
		var udf functions.FlatMapFunc
		decoder.Decode(&udf)
		ret = NewFlatMap(udf)
		return
	}
}

func NewFlatMap(function functions.FlatMapFunc) *FlatMap {
	return &FlatMap{function}
}

func (m *FlatMap) handleRecord(record types.Record, out utils.Emitter) {
	m.function.FlatMap(record, out)
}

func (m *FlatMap) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *FlatMap) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
