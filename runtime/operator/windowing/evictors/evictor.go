package evictors

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing"
)

type EvictorContext interface {
	GetCurrentProcessingTime() time.Time
	GetCurrentEventTime() time.Time
}

type Evictor interface {
	EvictBefore(coll *windowing.WindowCollection, size int64)
	EvictAfter(coll *windowing.WindowCollection, size int64)
}
