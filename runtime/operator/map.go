package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type Map struct {
	function funcintfs.Map
}

func NewMap(function funcintfs.Map) *Map {
	if function == nil {
		panic("map function must not be nil")
	}
	return &Map{function}
}

func (m *Map) New() intfs.Operator {
	udf := m.newFunction()
	return NewMap(udf)
}

func (m *Map) newFunction() (udf funcintfs.Map) {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (m *Map) handleRecord(record types.Record, out Emitter) {
	out.Emit(m.function.Map(record))
}

func (m *Map) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Map) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
