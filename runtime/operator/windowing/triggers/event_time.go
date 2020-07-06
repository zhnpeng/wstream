package triggers

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

type EventTimeTrigger struct {
}

func NewEventTimeTrigger() *EventTimeTrigger {
	return &EventTimeTrigger{}
}

func (trigger *EventTimeTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnProcessingTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}

func (trigger *EventTimeTrigger) OnEventTime(t time.Time, window windows.Window) TriggerSignal {
	if !t.Before(window.End()) {
		return FIRE
	}
	return CONTINUE
}
