package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type FlatMap struct {
	function functions.FlatMap
}

func NewFlatMap(function functions.FlatMap) *FlatMap {
	if function == nil {
		panic("flatmap function must not be nil")
	}
	return &FlatMap{function}
}

func (m *FlatMap) New() intfs.Operator {
	udf := m.newFunction()
	return NewFlatMap(udf)
}

func (m *FlatMap) newFunction() (udf functions.FlatMap) {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (m *FlatMap) handleRecord(record types.Record, out Emitter) {
	m.function.FlatMap(record, out)
}

func (m *FlatMap) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *FlatMap) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
