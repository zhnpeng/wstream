package operator

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPunctuatedWatermark struct {
	function          functions.TimestampWithPunctuatedWatermar
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func NewAssignTimestampWithPunctuatedWatermark(function functions.TimestampWithPunctuatedWatermar) *AssignTimestampWithPunctuatedWatermark {
	if function == nil {
		panic("TimestampWithPunctuatedWatermar function must not be nil")
	}
	return &AssignTimestampWithPunctuatedWatermark{
		function:      function,
		prevWatermark: &types.Watermark{},
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) New() intfs.Operator {
	udf := f.newFunction()
	return NewAssignTimestampWithPunctuatedWatermark(udf)
}

func (f *AssignTimestampWithPunctuatedWatermark) newFunction() (udf functions.TimestampWithPunctuatedWatermar) {
	encodedBytes := encodeFunction(f.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (f *AssignTimestampWithPunctuatedWatermark) handleRecord(record types.Record, out Emitter) {
	extractedTimestamp := f.function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	// get watermark
	nextWatermark := f.function.GetNextWatermark(record, extractedTimestamp)
	if nextWatermark != nil && nextWatermark.After(f.prevWatermark) {
		f.prevWatermark = nextWatermark
		out.Emit(nextWatermark)
	}
	// emit watermark before record
	out.Emit(record)
}

func (f *AssignTimestampWithPunctuatedWatermark) handleWatermark(wm *types.Watermark, out Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) Run(in Receiver, out Emitter) {
	consume(in, out, f)
}
