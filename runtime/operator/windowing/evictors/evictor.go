package evictors

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing"
)

type EvictorContext interface {
	GetCurrentProcessingTime() time.Time
	GetCurrentEventTime() time.Time
}

type Evictor interface {
	EvictBefore(contents *windowing.WindowContents, size int64)
	EvictAfter(contents *windowing.WindowContents, size int64)
}
