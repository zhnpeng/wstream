package operator

import (
	"bytes"
	"context"
	"encoding/gob"
	"sync"
	"time"

	"github.com/wandouz/wstream/runtime/execution"
	"github.com/wandouz/wstream/runtime/utils"

	"github.com/wandouz/wstream/functions"
	"github.com/wandouz/wstream/types"
)

type AssignTimestampWithPeriodicWatermark struct {
	function          functions.TimestampWithPeriodicWatermark
	period            time.Duration
	prevItemTimestamp int64
	prevWatermark     *types.Watermark
}

func GenAssignTimestampWithPeriodicWatermar(
	function functions.TimestampWithPeriodicWatermark,
	period time.Duration,
) func() execution.Operator {
	encodedBytes := encodeFunction(function)
	return func() (ret execution.Operator) {
		reader := bytes.NewReader(encodedBytes)
		decoder := gob.NewDecoder(reader)
		var udf functions.TimestampWithPeriodicWatermark
		err := decoder.Decode(&udf)
		if err != nil {
			panic(err)
		}
		ret = NewAssignTimestampWithPeriodicWatermark(udf, period)
		return
	}
}

func NewAssignTimestampWithPeriodicWatermark(
	function functions.TimestampWithPeriodicWatermark,
	period time.Duration,
) *AssignTimestampWithPeriodicWatermark {
	return &AssignTimestampWithPeriodicWatermark{
		function:      function,
		period:        period,
		prevWatermark: &types.Watermark{},
	}
}

func (f *AssignTimestampWithPeriodicWatermark) handleRecord(record types.Record, out utils.Emitter) {
	extractedTimestamp := f.function.ExtractTimestamp(record, f.prevItemTimestamp)
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

func (f *AssignTimestampWithPeriodicWatermark) Run(in *execution.Receiver, out utils.Emitter) {
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
