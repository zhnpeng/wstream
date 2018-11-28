package assigners

import (
	"github.com/wandouz/wstream/runtime/operator/windowing/triggers"
	"github.com/wandouz/wstream/runtime/operator/windowing/windows"
	"github.com/wandouz/wstream/types"
)

type GlobalWindow struct{}

func NewGlobalWindow() *GlobalWindow {
	return &GlobalWindow{}
}

// AssignWindows return all windows item was assigned to
func (w *GlobalWindow) AssignWindows(item types.Item, ctx AssignerContext) []windows.Window {
	return []windows.Window{windows.GetGlobalWindow()}
}

func (w *GlobalWindow) GetDefaultTrigger() triggers.Trigger {
	return &triggers.NeverTrigger{}
}

func (w *GlobalWindow) IsEventTime() bool {
	return false
}
