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

func NewCountTrigger() *CountTrigger {
	return &CountTrigger{}
}

func (trigger *CountTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	c := atomic.AddInt64(&trigger.count, 1)
	if c >= trigger.maxCount {
		return FIRE
	}
	return CONTINUE
}

func (trigger *CountTrigger) OnProcessingTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}

func (trigger *CountTrigger) OnEventTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}

func (trigger *CountTrigger) Of(maxCount int64) *CountTrigger {
	trigger.maxCount = maxCount
	return trigger
}
