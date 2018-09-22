package operator

import (
	"time"

	"github.com/wandouz/wstream/runtime/execution"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPunctuatedWatermark struct {
	Function          functions.TimestampWithPunctuatedWatermar
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func (f *AssignTimestampWithPunctuatedWatermark) handleRecord(record types.Record, out Emitter) {
	extractedTimestamp := f.Function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	out.Emit(record)
	// get watermark
	nextWatermark := f.Function.GetNextWatermark(record, extractedTimestamp)
	if nextWatermark != nil && nextWatermark.After(f.prevWatermark) {
		f.prevWatermark = nextWatermark
		out.Emit(nextWatermark)
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) handleWatermark(wm *types.Watermark, out Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *AssignTimestampWithPunctuatedWatermark) Run(in *execution.Receiver, out Emitter) {
	consume(in, out, f)
}
