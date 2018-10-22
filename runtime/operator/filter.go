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
	if function == nil {
		panic("filter function must not be nil")
	}
	return &Filter{function}
}

func (f *Filter) New() intfs.Operator {
	udf := f.newFunction()
	return NewFilter(udf)
}

func (f *Filter) newFunction() (udf functions.FilterFunc) {
	encodedBytes := encodeFunction(f.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (f *Filter) handleRecord(record types.Record, out Emitter) {
	if f.function.Filter(record) {
		out.Emit(record)
	}
}

func (f *Filter) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (f *Filter) Run(in Receiver, out Emitter) {
	consume(in, out, f)
}
