package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// Reduce is a rolling reduce in datastream and keyedstream
type Reduce struct {
	function         functions.ReduceFunc
	keyedAccumulator map[utils.KeyID]types.Record
}

func NewReduce(function functions.ReduceFunc) *Reduce {
	return &Reduce{
		function:         function,
		keyedAccumulator: make(map[utils.KeyID]types.Record),
	}
}

func (m *Reduce) New() utils.Operator {
	encodedBytes := encodeFunction(m.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	var udf functions.ReduceFunc
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return NewReduce(udf)
}

func (m *Reduce) handleRecord(record types.Record, out Emitter) {
	keys := utils.HashSlice(record.Key())
	if acc, ok := m.keyedAccumulator[keys]; ok {
		acc = m.function.Reduce(acc, record)
		m.keyedAccumulator[keys] = acc.Inherit(record)
	} else {
		m.keyedAccumulator[keys] = record
	}
	out.Emit(m.keyedAccumulator[keys])
}

func (m *Reduce) handleWatermark(wm *types.Watermark, out Emitter) {
	out.Emit(wm)
}

func (m *Reduce) Run(in Receiver, out Emitter) {
	consume(in, out, m)
}
