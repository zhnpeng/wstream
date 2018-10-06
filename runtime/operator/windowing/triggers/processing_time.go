package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type ProcessingTimeTrigger struct {
}

func (trigger *ProcessingTimeTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	ctx.RegisterProcessingTimer(window.MaxTimestamp())
	return CONTINUE
}

func (trigger *ProcessingTimeTrigger) OnProcessingTime(t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	return FIRE
}

func (trigger *ProcessingTimeTrigger) OnEventTime(t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	return CONTINUE
}
