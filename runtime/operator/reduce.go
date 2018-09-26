package operator

import (
	"bytes"
	"encoding/gob"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"
	"github.com/wandouz/wstream/types"
)

// Reduce is a rolling reduce in datastream and keyedstream
type Reduce struct {
	function    functions.ReduceFunc
	accumulator types.Record
}

func GenReduce(function functions.ReduceFunc) func() execution.Operator {
	encodedBytes := encodeFunction(function)
	return func() (ret execution.Operator) {
		reader := bytes.NewReader(encodedBytes)
		decoder := gob.NewDecoder(reader)
		var udf functions.ReduceFunc
		err := decoder.Decode(&udf)
		if err != nil {
			panic(err)
		}
		ret = NewReduce(udf)
		return
	}
}

func NewReduce(function functions.ReduceFunc) *Reduce {
	return &Reduce{
		function:    function,
		accumulator: function.InitialAccmulator(),
	}
}

func (m *Reduce) handleRecord(record types.Record, out utils.Emitter) {
	m.accumulator = m.function.Reduce(m.accumulator, record)
	out.Emit(m.accumulator)
}

func (m *Reduce) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	out.Emit(wm)
}

func (m *Reduce) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, m)
}
