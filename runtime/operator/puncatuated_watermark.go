package operator

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPunctuatedWatermark struct {
	function          functions.TimestampWithPunctuatedWatermar
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func GenAssignTimestampWithPunctuatedWatermar(
	function functions.TimestampWithPunctuatedWatermar,
) func() execution.Operator {
	encodedBytes := encodeFunction(function)
	return func() (ret execution.Operator) {
		reader := bytes.NewReader(encodedBytes)
		decoder := gob.NewDecoder(reader)
		var udf functions.TimestampWithPunctuatedWatermar
		err := decoder.Decode(&udf)
		if err != nil {
			panic(err)
		}
		ret = NewAssignTimestampWithPunctuatedWatermark(udf)
		return
	}
}

func NewAssignTimestampWithPunctuatedWatermark(function functions.TimestampWithPunctuatedWatermar) *AssignTimestampWithPunctuatedWatermark {
	return &AssignTimestampWithPunctuatedWatermark{
		function:      function,
		prevWatermark: &types.Watermark{},
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) handleRecord(record types.Record, out utils.Emitter) {
	extractedTimestamp := f.function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	out.Emit(record)
	// get watermark
	nextWatermark := f.function.GetNextWatermark(record, extractedTimestamp)
	if nextWatermark != nil && nextWatermark.After(f.prevWatermark) {
		f.prevWatermark = nextWatermark
		out.Emit(nextWatermark)
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) Run(in *execution.Receiver, out utils.Emitter) {
	consume(in, out, f)
}
