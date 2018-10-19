package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type FlatMap struct {
	function functions.FlatMapFunc
}

func NewFlatMap(function functions.FlatMapFunc) *FlatMap {
	return &FlatMap{function}
}
func (m *FlatMap) New() intfs.Operator {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	var udf functions.FlatMapFunc
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return NewFlatMap(udf)
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
