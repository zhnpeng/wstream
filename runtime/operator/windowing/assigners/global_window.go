package assigners

import (
	"time"

	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type GlobalWindow struct{}

func NewGlobalWindow() *GlobalWindow {
	return &GlobalWindow{}
}

type NeverTrigger struct {
}

func (t *NeverTrigger) OnItem(types.Item, time.Time, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) OnProcessingTime(time.Time, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) OnEventTime(time.Time, windows.Window, *triggers.TriggerContext) triggers.TriggerSignal {
	return triggers.CONTINUE
}

func (t *NeverTrigger) IsEventTime() bool {
	return false
}

func (t *NeverTrigger) Dispose() {}

// AssignWindows return all windows item was assigned to
func (w *GlobalWindow) AssignWindows(item types.Item) []windows.Window {
	return []windows.Window{windows.GetGlobalWindow()}
}

func (w *GlobalWindow) GetDefaultTrigger() triggers.Trigger {
	return &NeverTrigger{}
}

func (w *GlobalWindow) IsEventTime() bool {
	return false
}
