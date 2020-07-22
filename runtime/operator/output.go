package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type Output struct {
	function funcintfs.Output
}

func NewOutput(function funcintfs.Output) *Output {
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
	m.function.Output(record)
	out.Emit(record)
}

func (m *Output) newFunction() (udf funcintfs.Output) {
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
