package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type EventTimeTrigger struct {
}

func (t *EventTimeTrigger) OnItem(item types.Item, timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	// TODO inplement me
	return CONTINUE
}

func (t *EventTimeTrigger) OnProcessingTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	if timestamp == window.MaxTimestamp() {
		return FIRE
	}
	return CONTINUE
}

func (t *EventTimeTrigger) OnEventTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}
