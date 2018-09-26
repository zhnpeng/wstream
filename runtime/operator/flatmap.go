package operator

import (
	"bytes"
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
	encodedBytes := encodeFunction(function)
	return func() (ret execution.Operator) {
		reader := bytes.NewReader(encodedBytes)
		decoder := gob.NewDecoder(reader)
		var udf functions.FlatMapFunc
		err := decoder.Decode(&udf)
		if err != nil {
			panic(err)
		}
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
