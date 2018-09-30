package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type ProcessingTimeTrigger struct {
}

func (t *ProcessingTimeTrigger) OnItem(item types.Item, timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *ProcessingTimeTrigger) OnProcessingTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return FIRE
}

func (t *ProcessingTimeTrigger) OnEventTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}
