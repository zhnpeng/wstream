package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type Rescale struct {
	function funcintfs.Select
}

func NewRescale(function funcintfs.Select) *Rescale {
	if function == nil {
		panic("Rescale function must not be nil")
	}
	return &Rescale{function}
}

func (this *Rescale) New() intfs.Operator {
	udf := this.newFunction()
	return NewRescale(udf)
}

func (this *Rescale) newFunction() (udf funcintfs.Select) {
	encodedBytes := encodeFunction(this.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (this *Rescale) handleRecord(record types.Record, out Emitter) {
	index := this.function.Select(record, out.Length())
	out.EmitTo(index, record)
}

func (this *Rescale) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (this *Rescale) Run(in Receiver, out Emitter) {
	debugConsume(in, out, this)
}
