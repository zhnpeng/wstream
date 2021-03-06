package operator

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/zhnpeng/wstream/funcintfs"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type TimeWithPunctuatedWatermarkAssigner struct {
	function          funcintfs.TimestampWithPunctuatedWatermark
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func NewTimeWithPunctuatedWatermarkAssigner(function funcintfs.TimestampWithPunctuatedWatermark) *TimeWithPunctuatedWatermarkAssigner {
	if function == nil {
		panic("TimestampWithPunctuatedWatermar function must not be nil")
	}
	return &TimeWithPunctuatedWatermarkAssigner{
		function:      function,
		prevWatermark: &types.Watermark{},
	}
}

func (f *TimeWithPunctuatedWatermarkAssigner) New() intfs.Operator {
	udf := f.newFunction()
	return NewTimeWithPunctuatedWatermarkAssigner(udf)
}

func (f *TimeWithPunctuatedWatermarkAssigner) newFunction() (udf funcintfs.TimestampWithPunctuatedWatermark) {
	encodedBytes := encodeFunction(f.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (f *TimeWithPunctuatedWatermarkAssigner) handleRecord(record types.Record, out Emitter) {
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

func (f *TimeWithPunctuatedWatermarkAssigner) handleWatermark(wm *types.Watermark, out Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *TimeWithPunctuatedWatermarkAssigner) Run(in Receiver, out Emitter) {
	consume(in, out, f)
}
