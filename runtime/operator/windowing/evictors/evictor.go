package evictors

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type EvictorContext interface {
	GetCurrentProcessingTime() time.Time
	GetCurrentEventTime() time.Time
}

type Evictor interface {
	EvictBefore(windows.Window, EvictorContext)
	EvictAfter(windows.Window, EvictorContext)
}

type Iterable interface {
	Next() types.Record
	HasNext() bool
	Remove()
}
