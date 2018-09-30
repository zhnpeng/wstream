package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type PurgingTrigger struct {
}

func (t *PurgingTrigger) OnItem(item types.Item, timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *PurgingTrigger) OnProcessingTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *PurgingTrigger) OnEventTime(timestamp time.Duration, window windows.Window, ctx *TriggerContext) TriggerSignal {
	return CONTINUE
}
