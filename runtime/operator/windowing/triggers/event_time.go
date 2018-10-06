package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type EventTimeTrigger struct {
}

func (trigger *EventTimeTrigger) OnItem(item types.Item, timestamp time.Duration, window windows.Window, ctx TriggerContext) TriggerSignal {
	// TODO inplement me
	if window.MaxTimestamp().Before(ctx.GetCurrentEventTime()) {
		return FIRE
	}
	ctx.RegisterEventTimer(window.MaxTimestamp())
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnProcessingTime(t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	if t.After(window.MaxTimestamp()) {
		return FIRE
	}
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnEventTime(t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	return CONTINUE
}
