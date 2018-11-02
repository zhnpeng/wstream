package functions

import (
	"time"

	"github.com/wandouz/wstream/types"
)

type TimestampWithPunctuatedWatermar interface {
	// ExtractTime return sec only so minious granularity is second
	ExtractTimestamp(record types.Record, prevRecordTimestamp int64) (sec int64)
	GetNextWatermark(record types.Record, extractedTimestamp int64) (wm *types.Watermark)
}

type AssignTimeWithPeriodicWatermark interface {
	ExtractTimestamp(record types.Record, prevRecordTimestamp int64) (sec int64)
	GetNextWatermark() (wm *types.Watermark)
	Period() time.Duration
}
