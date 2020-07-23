package funcintfs

import (
	"time"

	"github.com/zhnpeng/wstream/types"
)

type TimestampWithPunctuatedWatermar interface {
	// ExtractTime set record's time to event time, return sec only so minious granularity is second
	ExtractTimestamp(record types.Record, prevRecordTimestamp int64) (sec int64)
	GetNextWatermark(record types.Record, extractedTimestamp int64) (wm *types.Watermark)
}

type AssignTimeWithPeriodicWatermark interface {
	// ExtractTime set record's time to event time, return sec only so minious granularity is second
	ExtractTimestamp(record types.Record, prevRecordTimestamp int64) (sec int64)
	GetNextWatermark(t time.Time) (wm *types.Watermark)
	Period() time.Duration
}
