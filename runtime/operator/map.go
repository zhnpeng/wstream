package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Map struct {
	function functions.MapFunc
}

func NewMap(function functions.MapFunc) *Map {
	return &Map{function}
}

func (m *Map) New() execution.Operator {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	var udf functions.MapFunc
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return NewMap(udf)
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
