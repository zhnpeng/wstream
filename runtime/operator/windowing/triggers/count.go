package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type CountTrigger struct {
	maxCount int
}

func NewCountTrigger() *CountTrigger {
	return &CountTrigger{}
}

func (trigger *CountTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	c := ctx.WindowSize()
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

func (trigger *CountTrigger) Of(maxCount int) *CountTrigger {
	trigger.maxCount = maxCount
	return trigger
}
