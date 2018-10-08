package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type PurgingTrigger struct {
}

func (trigger *PurgingTrigger) OnItem(item types.Item, t time.Time, window windows.Window, ctx TriggerContext) TriggerSignal {
	return CONTINUE
}

func (trigger *PurgingTrigger) OnProcessingTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}

func (trigger *PurgingTrigger) OnEventTime(t time.Time, window windows.Window) TriggerSignal {
	return CONTINUE
}
