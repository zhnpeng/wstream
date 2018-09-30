package triggers

import (
	"sync/atomic"
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type CountTrigger struct {
	maxCount int64
	count    int64
}

func NewCountTrigger(maxCount int64) *CountTrigger {
	return &CountTrigger{
		maxCount: maxCount,
	}
}

func (t *CountTrigger) OnItem(item types.Item, timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	c := atomic.AddInt64(&t.count, 1)
	if c >= t.maxCount {
		return FIRE
	}
	return CONTINUE
}

func (t *CountTrigger) OnProcessingTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *CountTrigger) OnEventTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *CountTrigger) Of(maxCount int64) *CountTrigger {
	t.maxCount = maxCount
	return t
}
