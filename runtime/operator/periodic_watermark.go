package operator

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/intfs"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPeriodicWatermark struct {
	function          functions.TimestampWithPeriodicWatermark
	period            time.Duration
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func NewAssignTimestampWithPeriodicWatermark(
	function functions.TimestampWithPeriodicWatermark,
	period time.Duration,
) *AssignTimestampWithPeriodicWatermark {
	if function == nil {
		panic("TimestampWithPeriodWatermark function must not be nil")
	}
	return &AssignTimestampWithPeriodicWatermark{
		function:      function,
		period:        period,
		prevWatermark: &types.Watermark{},
	}
}

func (f *AssignTimestampWithPeriodicWatermark) New() intfs.Operator {
	udf := f.newFunction()
	return NewAssignTimestampWithPeriodicWatermark(udf, f.period)
}

func (f *AssignTimestampWithPeriodicWatermark) newFunction() (udf functions.TimestampWithPeriodicWatermark) {
	encodedBytes := encodeFunction(f.function)
	reader := bytes.NewReader(encodedBytes)
	decoder := gob.NewDecoder(reader)
	err := decoder.Decode(&udf)
	if err != nil {
		panic(err)
	}
	return
}

func (f *AssignTimestampWithPeriodicWatermark) handleRecord(record types.Record, out Emitter) {
	extractedTimestamp := f.function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	out.Emit(record)
}

func (f *AssignTimestampWithPeriodicWatermark) handleWatermark(wm *types.Watermark, out Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *AssignTimestampWithPeriodicWatermark) Run(in Receiver, out Emitter) {
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
