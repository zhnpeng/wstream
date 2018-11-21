package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
	"github.com/wandouz/wstream/utils"
)

// Reduce is a rolling reduce in datastream and keyedstream
type Reduce struct {
	function         functions.Reduce
	keyedAccumulator map[utils.KeyID]types.Record
}

func NewReduce(function functions.Reduce) *Reduce {
	if function == nil {
		panic("reduce function must not be nil")
	}
	return &Reduce{
		function:         function,
		keyedAccumulator: make(map[utils.KeyID]types.Record),
	}
}

func (m *Reduce) New() intfs.Operator {
	udf := m.newFunction()
	return NewReduce(udf)
}

func (m *Reduce) newFunction() (udf functions.Reduce) {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (m *Reduce) handleRecord(record types.Record, out Emitter) {
	keys := utils.HashSlice(record.Key())
	var acc types.Record
	if prevAcc, ok := m.keyedAccumulator[keys]; ok {
		acc = prevAcc
	}
	acc = m.function.Reduce(acc, record)
	m.keyedAccumulator[keys] = acc.Inherit(record)
	out.Emit(m.keyedAccumulator[keys])
}

func (m *Reduce) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Reduce) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
