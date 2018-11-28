package triggers

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type NeverTrigger struct {
}

func (t *NeverTrigger) OnItem(types.Item, time.Time, windows.Window, TriggerContext) TriggerSignal {
	return CONTINUE
}

func (t *NeverTrigger) OnProcessingTime(time.Time, windows.Window) TriggerSignal {
	return CONTINUE
}

func (t *NeverTrigger) OnEventTime(time.Time, windows.Window) TriggerSignal {
	return CONTINUE
}
