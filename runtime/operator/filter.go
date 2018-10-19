package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type Filter struct {
	function functions.FilterFunc
}

func NewFilter(function functions.FilterFunc) *Filter {
	return &Filter{function}
}

func (m *Filter) New() intfs.Operator {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	var udf functions.FilterFunc
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return NewFilter(udf)
}

func (m *Filter) handleRecord(record types.Record, out Emitter) {
	if m.function.Filter(record) {
		out.Emit(record)
	}
}

func (m *Filter) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Filter) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
