package triggers

import (
	"time"

	"github.com/zhnpeng/wstream/runtime/operator/windowing/windows"
	"github.com/zhnpeng/wstream/types"
)

type ProcessingTimeTrigger struct {
}

func NewProcessingTimeTrigger() *ProcessingTimeTrigger {
	return &ProcessingTimeTrigger{}
}

func (trigger *ProcessingTimeTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	// ctx.RegisterProcessingTimer(window.MaxTimestamp())
	return CONTINUE
}

func (trigger *ProcessingTimeTrigger) OnProcessingTime(t time.Time, window windows.Window) TriggerSignal {
	return FIRE
}

func (trigger *ProcessingTimeTrigger) OnEventTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}
