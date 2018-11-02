package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type Output struct {
	function functions.Output
}

func NewOutput(function functions.Output) *Output {
	if function == nil {
		panic("output function must not be nil")
	}
	return &Output{function}
}

func (m *Output) New() intfs.Operator {
	udf := m.newFunction()
	return NewOutput(udf)
}

func (m *Output) handleRecord(record types.Record, out Emitter) {
	m.function.Output(record.Copy())
	out.Emit(record)
}

func (m *Output) newFunction() (udf functions.Output) {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (m *Output) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Output) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
