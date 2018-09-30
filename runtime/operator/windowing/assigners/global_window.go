package assigners

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type GlobalWindow struct{}

type NeverTrigger struct {
}

func (t *NeverTrigger) OnItem(types.Item, time.Duration, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) OnProcessingTime(time.Duration, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) OnEventTime(time.Duration, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) Dispose() {}

// AssignWindows return all windows item was assigned to
func AssignWindows(item types.Item) []windows.Window {
	return []windows.Window{windows.GetGlobalWindow()}
}

func GetTrigger() triggers.Trigger {
	return &NeverTrigger{}
}
