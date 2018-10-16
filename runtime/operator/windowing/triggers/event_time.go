package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type EventTimeTrigger struct {
}

func NewEventTimeTrigger() *EventTimeTrigger {
	return &EventTimeTrigger{}
}

func (trigger *EventTimeTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	if window.MaxTimestamp().Before(ctx.GetCurrentEventTime()) {
		return FIRE
	}
	ctx.RegisterEventTimer(window.MaxTimestamp())
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnProcessingTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnEventTime(t time.Time, window windows.Window) TriggerSignal {
	if t.Equal(window.MaxTimestamp()) {
		return FIRE
	}
	return CONTINUE
}
