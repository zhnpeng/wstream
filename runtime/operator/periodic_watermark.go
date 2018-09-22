package operator

import (
	"context"
	"sync"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPeriodicWatermark struct {
	Function          functions.TimestampWithPeriodicWatermark
	period            time.Duration
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func (f *AssignTimestampWithPeriodicWatermark) handleRecord(record types.Record, out utils.Emitter) {
	extractedTimestamp := f.Function.ExtractTimestamp(record, f.prevItemTimestamp)
	f.prevItemTimestamp = extractedTimestamp
	record.SetTime(time.Unix(extractedTimestamp, 0))
	out.Emit(record)
}

func (f *AssignTimestampWithPeriodicWatermark) handleWatermark(wm *types.Watermark, out utils.Emitter) {
	if wm != nil && wm.After(f.prevWatermark) {
		f.prevWatermark = wm
		out.Emit(wm)
	}
}

func (f *AssignTimestampWithPeriodicWatermark) Run(ctx context.Context, in *execution.Receiver, out utils.Emitter) {
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
				nextWatermark := f.Function.GetNextWatermark()
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
