package operator

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/zhnpeng/wstream/functions"
	"github.com/zhnpeng/wstream/intfs"
	"github.com/zhnpeng/wstream/types"
)

type TimeWithPeriodicWatermarkAssigner struct {
	function          functions.AssignTimeWithPeriodicWatermark
	period            time.Duration
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func NewTimeWithPeriodicWatermarkAssigner(
	function functions.AssignTimeWithPeriodicWatermark,
	period time.Duration,
) *TimeWithPeriodicWatermarkAssigner {
	if function == nil {
		panic("TimestampWithPeriodWatermark function must not be nil")
	}
	return &TimeWithPeriodicWatermarkAssigner{
		function:      function,
		period:        period,
		prevWatermark: &types.Watermark{},
	}
}

func (f *TimeWithPeriodicWatermarkAssigner) New() intfs.Operator {
	udf := f.newFunction()
	return NewTimeWithPeriodicWatermarkAssigner(udf, f.period)
}

func (f *TimeWithPeriodicWatermarkAssigner) newFunction() (udf functions.AssignTimeWithPeriodicWatermark) {
	encodedBytes := encodeFunction(f.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (f *TimeWithPeriodicWatermarkAssigner) handleRecord(record types.Record, out Emitter) {
	extractedTimestamp := f.function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	out.Emit(record)
}

func (f *TimeWithPeriodicWatermarkAssigner) handleWatermark(wm *types.Watermark, out Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *TimeWithPeriodicWatermarkAssigner) Run(in Receiver, out Emitter) {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(f.period)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				nextWatermark := f.function.GetNextWatermark()
				f.handleWatermark(nextWatermark, out)
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		consume(in, out, f)
	}()

	wg.Wait()
}
