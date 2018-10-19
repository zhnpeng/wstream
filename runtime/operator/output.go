package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

type Output struct {
	function functions.OutputFunc
}

func NewOutput(function functions.OutputFunc) *Output {
	return &Output{function}
}

func (m *Output) New() utils.Operator {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	var udf functions.OutputFunc
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return NewOutput(udf)
}

func (m *Output) handleRecord(record types.Record, out Emitter) {
	m.function.Output(record.Copy())
	out.Emit(record)
}

func (m *Output) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Output) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
